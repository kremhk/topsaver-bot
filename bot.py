import asyncio, re, os, hashlib, subprocess, shlex
from dataclasses import dataclass
from contextlib import suppress

from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.utils.chat_action import ChatActionSender

import yt_dlp
import redis
from dotenv import load_dotenv

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
REQUIRE_SUBSCRIBE = os.getenv("REQUIRE_SUBSCRIBE","0") == "1"
REQUIRED_CHANNEL = os.getenv("REQUIRED_CHANNEL","")

r = redis.from_url(REDIS_URL, decode_responses=True)
bot = Bot(BOT_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher()

URL_RE = re.compile(r"(https?://\S+)")
DATA_DIR = "data"

def key_for(url:str, kind:str)->str:
    h = hashlib.md5(url.encode()).hexdigest()
    return f"cache:{kind}:{h}"

def cached_path(url:str, kind:str)->str|None:
    p = r.get(key_for(url, kind))
    return p if p and os.path.exists(p) else None

def save_cache(url:str, kind:str, path:str):
    r.set(key_for(url, kind), path, ex=60*60*24*3)  # 3 дня

def is_download_task_active(user_id:int)->bool:
    return r.setnx(f"lock:{user_id}", "1") == 0

def set_lock(user_id:int, ttl=300):
    r.set(f"lock:{user_id}", "1", ex=ttl)

def release_lock(user_id:int):
    r.delete(f"lock:{user_id}")

def sanitize_filename(s:str)->str:
    return re.sub(r"[^\w\-. ]", "_", s).strip()[:180]

async def check_subscription(user_id:int)->bool:
    if not REQUIRE_SUBSCRIBE or not REQUIRED_CHANNEL:
        return True
    with suppress(Exception):
        m = await bot.get_chat_member(REQUIRED_CHANNEL, user_id)
        return m.status in ("member","administrator","creator")
    return False

def ydl_opts_audio(out_tmpl:str):
    return {
        "outtmpl": out_tmpl,
        "format": "bestaudio/best",
        "postprocessors": [
            {"key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": "192"}
        ],
        "noplaylist": True,
        "quiet": True,
        "nocheckcertificate": True
    }

def ydl_opts_video(out_tmpl:str):
    return {
        "outtmpl": out_tmpl,
        "format": "mp4/best",
        "merge_output_format": "mp4",
        "noplaylist": True,
        "quiet": True,
        "nocheckcertificate": True
    }

def download(url:str, kind:str)->tuple[str,str]:
    os.makedirs(DATA_DIR, exist_ok=True)
    out = os.path.join(DATA_DIR, "%(title)s.%(ext)s")
    opts = ydl_opts_audio(out) if kind=="audio" else ydl_opts_video(out)

    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=True)
        title = sanitize_filename(info.get("title") or "file")
        # yt-dlp returns exact filename via prepare_filename
        file_path = ydl.prepare_filename(info)
        if kind == "audio" and not file_path.endswith(".mp3"):
            base, _ = os.path.splitext(file_path)
            mp3 = base + ".mp3"
            if os.path.exists(mp3):
                file_path = mp3
        # rename to sanitized
        new_path = os.path.join(DATA_DIR, f"{title}{os.path.splitext(file_path)[1]}")
        if file_path != new_path:
            with suppress(FileNotFoundError):
                os.replace(file_path, new_path)
        return new_path, title

def human(x:int)->str:
    for unit in ["B","KB","MB","GB"]:
        if x<1024: return f"{x:.1f} {unit}"
        x/=1024
    return f"{x:.1f} TB"

def file_size(path:str)->int:
    try: return os.stat(path).st_size
    except: return 0

def build_choice_kb(url:str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎬 Видео", callback_data=f"dl:video:{url}")],
        [InlineKeyboardButton(text="🎧 Аудио (mp3)", callback_data=f"dl:audio:{url}")],
        [InlineKeyboardButton(text="🔗 Только ссылка", callback_data=f"dl:link:{url}")]
    ])

@dp.message(CommandStart())
async def start(m: Message):
    text = (
        "Привет! Кинь ссылку с YouTube / Instagram / TikTok / Pinterest / Likee — подготовлю файл.\n"
        "Выбери формат после отправки ссылки.\n"
        "💡 Большие файлы могу выслать как документ или вернуть ссылку на скачивание."
    )
    await m.answer(text)

@dp.message(Command("help"))
async def help_cmd(m: Message):
    await m.answer("Просто пришли ссылку. Если нужна только дорожка — выбери «Аудио (mp3)». Для больших файлов выбери «Только ссылка».")

@dp.message(F.text.regexp(URL_RE))
async def handle_url(m: Message):
    if not await check_subscription(m.from_user.id):
        await m.answer(f"Подпишись на {REQUIRED_CHANNEL} и пришли ссылку снова 🙏")
        return
    url = URL_RE.search(m.text).group(0)
    await m.answer("Нашёл ссылку. Что нужно вытянуть?", reply_markup=build_choice_kb(url))

@dp.callback_query(F.data.startswith("dl:"))
async def do_download(cq: CallbackQuery):
    _, kind, url = cq.data.split(":", 2)

    if is_download_task_active(cq.from_user.id):
        await cq.answer("У тебя уже идёт загрузка, подожди её окончания.", show_alert=True)
        return
    set_lock(cq.from_user.id, ttl=600)

    try:
        if kind == "link":
            await cq.message.answer(f"Вот исходная ссылка:\n{url}")
            await cq.answer()
            return

        cached = cached_path(url, kind)
        path, title = (cached, os.path.splitext(os.path.basename(cached))[0]) if cached else (None, None)

        async with ChatActionSender.upload_document(bot=bot, chat_id=cq.message.chat.id):
            if not path:
                # скачиваем
                path, title = await asyncio.to_thread(download, url, kind)
                save_cache(url, kind, path)

            size = file_size(path)
            cap = f"<b>{title}</b>\n{human(size)}"
            # Telegram Bot API обычно допускает до 2 ГБ (зависит от окружения)
            if size <= 1900 * 1024 * 1024:
                if kind == "audio":
                    await cq.message.answer_audio(audio=open(path, "rb"), caption=cap)
                else:
                    # пробуем как video, fallback — document
                    with suppress(Exception):
                        await cq.message.answer_video(video=open(path, "rb"), caption=cap)
                        await cq.answer()
                        return
                    await cq.message.answer_document(document=open(path, "rb"), caption=cap)
                await cq.answer()
            else:
                await cq.message.answer(
                    "Файл слишком большой для отправки через Bot API. Держи ссылку-источник:\n" + url
                )
                await cq.answer()
    except Exception as e:
        await cq.message.answer(f"😬 Ошибка: {e}\nПопробуй другой формат/ссылку.")
        with suppress(Exception):
            await bot.send_message(OWNER_ID, f"ERR from {cq.from_user.id}\n{e}")
    finally:
        release_lock(cq.from_user.id)

if __name__ == "__main__":
    asyncio.run(dp.start_polling(bot))
