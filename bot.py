import asyncio
import aiohttp
import time
import json
import os

# ================= KONFIGURASI ================= #
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "TOKEN_BOT_ANDA")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "CHAT_ID_ANDA")
STATE_FILE = "state.json"

# Aturan Logika Timeframe
RULES = [
    {"interval": "15", "max_mins": 7, "name": "15 Menit"},
    {"interval": "60", "max_mins": 22, "name": "1 Jam"},
    {"interval": "240", "max_mins": 65, "name": "4 Jam"},
    {"interval": "D", "max_mins": 720, "name": "1 Hari"}
]

# Maksimal request bersamaan ke Bybit agar tidak error 429 (Too Many Requests)
MAX_CONCURRENT_REQUESTS = 30 
# =============================================== #

async def send_telegram(session, message):
    """Kirim pesan ke Telegram dengan penanganan error"""
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        async with session.post(url, json=payload) as resp:
            if resp.status != 200:
                print(f"Gagal kirim Telegram: {await resp.text()}")
    except Exception as e:
        print(f"Error koneksi ke Telegram: {e}")

async def get_all_symbols(session):
    """Mengambil semua koin Futures USDT yang aktif di Bybit"""
    url = "https://api.bybit.com/v5/market/instruments-info"
    params = {"category": "linear"} # Kategori Futures
    symbols = []
    try:
        async with session.get(url, params=params) as resp:
            data = await resp.json()
            if data.get("retCode") == 0:
                for item in data["result"]["list"]:
                    # Hanya ambil pair USDT yang statusnya sedang Trading
                    if item["quoteCoin"] == "USDT" and item["status"] == "Trading":
                        symbols.append(item["symbol"])
            print(f"Berhasil mengambil {len(symbols)} koin futures USDT.")
            return symbols
    except Exception as e:
        print(f"Gagal mengambil daftar koin: {e}")
        return []

async def check_volume(session, symbol, rule, state, sem, alerts_queue):
    """Mengecek volume koin tertentu berdasarkan rule, dibatasi oleh Semaphore"""
    async with sem: # Menahan agar tidak terlalu banyak request di detik yang sama
        url = "https://api.bybit.com/v5/market/kline"
        params = {
            "category": "linear",
            "symbol": symbol,
            "interval": rule["interval"],
            "limit": 2
        }
        
        try:
            async with session.get(url, params=params) as resp:
                data = await resp.json()
        except Exception:
            return False # Skip jika error koneksi

        if not data or data.get("retCode") != 0:
            return False

        klines = data["result"]["list"]
        if len(klines) < 2:
            return False

        curr_candle = klines[0]
        prev_candle = klines[1]

        curr_open_time_ms = int(curr_candle[0])
        curr_usdt_vol = float(curr_candle[6])
        prev_usdt_vol = float(prev_candle[6])

        # Hitung usia candle berjalan dalam menit
        current_time_ms = int(time.time() * 1000)
        elapsed_minutes = (current_time_ms - curr_open_time_ms) / 60000

        # LOGIKA UTAMA
        if elapsed_minutes <= rule["max_mins"] and curr_usdt_vol >= prev_usdt_vol:
            
            # Buat struktur state per koin jika belum ada
            if symbol not in state:
                state[symbol] = {}

            # Cek apakah sudah pernah kirim alert untuk koin & timeframe & timestamp ini
            if state[symbol].get(rule["name"]) != str(curr_open_time_ms):
                
                msg = (
                    f"🚨 <b>VOLUME USDT SPIKE</b> 🚨\n\n"
                    f"💎 <b>Pair:</b> #{symbol}\n"
                    f"⏳ <b>Timeframe:</b> {rule['name']}\n\n"
                    f"Volume membalap TF sebelumnya dengan sangat cepat!\n"
                    f"⏱️ <b>Usia Candle:</b> {elapsed_minutes:.1f} menit / {rule['max_mins']} m\n"
                    f"📊 <b>Vol Sblm:</b> {prev_usdt_vol:,.0f} USDT\n"
                    f"📈 <b>Vol Skrg:</b> {curr_usdt_vol:,.0f} USDT"
                )
                alerts_queue.append(msg)
                
                # Update state
                state[symbol][rule["name"]] = str(curr_open_time_ms)
                return True # Menandakan state berubah

        return False

async def main():
    # 1. Baca data state sebelumnya
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            try:
                state = json.load(f)
            except:
                state = {}
    else:
        state = {}

    state_updated = False
    alerts_queue = []

    async with aiohttp.ClientSession() as session:
        # 2. Dapatkan semua koin
        symbols = await get_all_symbols(session)
        if not symbols:
            return

        # 3. Siapkan tugas pemindaian dengan pembatas kecepatan (Semaphore)
        sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        tasks = []
        
        for symbol in symbols:
            for rule in RULES:
                tasks.append(check_volume(session, symbol, rule, state, sem, alerts_queue))

        # 4. Jalankan semua tugas secara paralel
        print(f"Memulai {len(tasks)} pengecekan (TF 15m, 1h, 4h, 1D)...")
        results = await asyncio.gather(*tasks)
        
        if any(results):
            state_updated = True

        # 5. Eksekusi pengiriman Telegram
        if alerts_queue:
            print(f"Menemukan {len(alerts_queue)} sinyal! Mengirim ke Telegram...")
            for msg in alerts_queue:
                await send_telegram(session, msg)
                # Jeda 0.5 detik antar pesan agar tidak diblokir Telegram karena spam
                await asyncio.sleep(0.5) 
        else:
            print("Tidak ada sinyal baru yang memenuhi syarat.")

    # 6. Simpan state agar Github Actions tidak spam
    if state_updated:
        # Clean up data lama (opsional) agar file json tidak bengkak
        # (Di sini kita simpan apa adanya dulu karena file size teks sangat kecil)
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
        print("Data state.json berhasil diperbarui.")

if __name__ == "__main__":
    asyncio.run(main())
