import os
import time
import json
import asyncio
import requests
import httpx
import traceback
from datetime import datetime
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

load_dotenv()

# ======================
# Debug toggle
# ======================
DEBUG_BETCO = (os.getenv("DEBUG_BETCO", "1").lower() in ("1", "true", "yes", "on"))

# ======================
# Telegram ENV
# ======================
BOT_TOKEN = (os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN veya TELEGRAM_BOT_TOKEN .env içinde yok!")

# ======================
# Panel ENV (Bearer)
# ======================
PANEL_API_BASE = os.getenv("PANEL_API_BASE", "https://vipdashboard.netlify.app").rstrip("/")
PANEL_PAGE_SIZE = int(os.getenv("PANEL_PAGE_SIZE", "200"))
PANEL_TIMEOUT = int(os.getenv("PANEL_TIMEOUT", "12"))
PANEL_MAX_PAGES = int(os.getenv("PANEL_MAX_PAGES", "200"))
INDEX_TTL_SECONDS = int(os.getenv("INDEX_TTL_SECONDS", "600"))

# Panel API Bearer Token (senin yazdığın: Authorization: Bearer <BOT_API_TOKEN>)
PANEL_BOT_API_TOKEN = (os.getenv("PANEL_BOT_API_TOKEN") or os.getenv("BOT_API_TOKEN") or "").strip()

# Opsiyonel: Panelden Betco config çekmek istersen (kurduysan)
# Örn: https://vipdashboard.netlify.app/api/bot-config
PANEL_CONFIG_URL = (os.getenv("PANEL_CONFIG_URL") or "").strip()
CONFIG_TTL_SECONDS = int(os.getenv("CONFIG_TTL_SECONDS", "300"))

# ======================
# Telegram izin modeli: Grup bazlı
# ======================
ALLOWED_CHAT_IDS = {
    int(x) for x in os.getenv("ALLOWED_TELEGRAM_CHAT_IDS", "").replace(" ", "").split(",")
    if x.strip().lstrip("-").isdigit()
}

ALLOW_PRIVATE = (os.getenv("ALLOW_PRIVATE", "0").lower() in ("1", "true", "yes", "on"))

def is_allowed(update: Update) -> bool:
    chat = update.effective_chat
    if chat is None:
        return False

    # Private (DM) istemiyorsan kapat
    if chat.type == "private":
        return ALLOW_PRIVATE  # default False

    # Grup/Süpergrup: sadece izinli chat_id'lerde çalış
    if ALLOWED_CHAT_IDS:
        return chat.id in ALLOWED_CHAT_IDS

    # Eğer hiç chat_id tanımlamazsan: güvenli olsun diye kapalı kalsın
    return False

# ======================
# Betco (BetConstruct webadmin) ENV (fallback)
# ======================
API_BASE = os.getenv("BETCO_API_BASE", os.getenv("API_BASE", "https://backofficewebadmin.betconstruct.com/api/en")).strip().rstrip("/")

# Eğer yanlışlıkla domain kökü verilirse otomatik düzelt
if API_BASE.endswith("backofficewebadmin.betconstruct.com"):
    API_BASE = API_BASE + "/api/en"

API_COOKIES = os.getenv("BETCO_COOKIES", os.getenv("API_COOKIES", "")).strip()
API_LANG = os.getenv("BETCO_LANGUAGE", os.getenv("API_LANGUAGE", "en")).strip() or "en"
ORIGIN = os.getenv("BETCO_ORIGIN", os.getenv("ORIGIN", "https://backoffice.betconstruct.com")).strip()
REFERER = os.getenv("BETCO_REFERER", os.getenv("REFERER", "https://backoffice.betconstruct.com/")).strip()
USER_AGENT = os.getenv("BETCO_USER_AGENT", os.getenv("USER_AGENT", "Mozilla/5.0")).strip()
APP_VERSION = os.getenv("BETCO_APP_VERSION", os.getenv("APP_VERSION", "")).strip()
PARTNER_ID = os.getenv("BETCO_PARTNER_ID", os.getenv("PARTNER_ID", "")).strip()
VERIFY_SSL = os.getenv("BETCO_VERIFY_SSL", os.getenv("VERIFY_SSL", "false")).lower() in ("1", "true", "yes", "on")
BETCO_TIMEOUT = float(os.getenv("BETCO_TIMEOUT", "25"))

API_AUTHENTICATION = os.getenv("BETCO_AUTHENTICATION", os.getenv("API_AUTHENTICATION", "")).strip()
API_AUTHTOKEN = os.getenv("BETCO_AUTHTOKEN", os.getenv("API_AUTHTOKEN", "")).strip()
EXTRA_JSON = os.getenv("BETCO_EXTRA_HEADERS_JSON", os.getenv("EXTRA_HEADERS_JSON", "")).strip()

# ======================
# VIP logic
# ======================
VIP_ORDER = ["iron", "bronze", "silver", "gold", "plat", "diamond"]
VIP_TARGET_90D = {
    "bronze": 50_000,
    "silver": 100_000,
    "gold": 200_000,
    "plat": 500_000,
    "diamond": 2_000_000,
}
VIP_TR_NAME = {
    "iron": "Iron",
    "bronze": "Bronze",
    "silver": "Gümüş",
    "gold": "Altın",
    "plat": "Platin",
    "diamond": "Diamond",
}

# ======================
# Panel in-memory index (stale-while-revalidate)
# ======================
USER_INDEX: dict[str, dict] = {}
INDEX_EXPIRES_AT: float = 0.0
INDEX_LOCK = asyncio.Lock()

REFRESH_IN_FLIGHT: bool = False
REFRESH_LAST_START: float = 0.0
MIN_REFRESH_GAP_SECONDS = 5.0

# ======================
# Panel config cache (opsiyonel)
# ======================
PANEL_CFG: dict = {}
CFG_EXPIRES_AT: float = 0.0
CFG_LOCK = asyncio.Lock()

# ======================
# Betco cache (hız) - login bazlı
# ======================
BETCO_CACHE: dict[str, tuple[float, dict]] = {}
BETCO_CACHE_TTL = 120  # saniye



# ----------------------
# Panel HTTP helpers (Bearer)
# ----------------------
def _panel_headers() -> dict[str, str]:
    h = {}
    if PANEL_BOT_API_TOKEN:
        h["Authorization"] = f"Bearer {PANEL_BOT_API_TOKEN}"
    return h


def _get_json(url: str, params: dict | None = None, timeout: int = 12) -> dict:
    last_err = None
    headers = _panel_headers()
    for attempt in range(3):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(0.6 * (attempt + 1))
    raise last_err  # type: ignore


def _build_full_index_sync() -> dict[str, dict]:
    url = f"{PANEL_API_BASE}/api/vip-members"
    index: dict[str, dict] = {}

    first = _get_json(url, params={"page": 1, "pageSize": PANEL_PAGE_SIZE}, timeout=PANEL_TIMEOUT)
    if not first.get("ok"):
        return index

    total_pages = int(first.get("totalPages") or 0)
    total_pages = min(total_pages, PANEL_MAX_PAGES)

    for item in (first.get("items") or []):
        u = item.get("username")
        if u:
            index[u] = item

    for page in range(2, total_pages + 1):
        data = _get_json(url, params={"page": page, "pageSize": PANEL_PAGE_SIZE}, timeout=PANEL_TIMEOUT)
        if not data.get("ok"):
            continue
        for item in (data.get("items") or []):
            u = item.get("username")
            if u:
                index[u] = item

    return index


def _index_is_stale() -> bool:
    return not USER_INDEX or time.time() >= INDEX_EXPIRES_AT


async def refresh_index(force: bool = False) -> bool:
    global USER_INDEX, INDEX_EXPIRES_AT, REFRESH_IN_FLIGHT, REFRESH_LAST_START

    if not force and not _index_is_stale():
        return False

    now = time.time()
    if REFRESH_IN_FLIGHT and (now - REFRESH_LAST_START) < INDEX_TTL_SECONDS:
        return False
    if (now - REFRESH_LAST_START) < MIN_REFRESH_GAP_SECONDS:
        return False

    async with INDEX_LOCK:
        now = time.time()
        if not force and not _index_is_stale():
            return False
        if (now - REFRESH_LAST_START) < MIN_REFRESH_GAP_SECONDS:
            return False

        REFRESH_IN_FLIGHT = True
        REFRESH_LAST_START = now
        try:
            new_index = await asyncio.to_thread(_build_full_index_sync)
            if new_index:
                USER_INDEX = new_index
                INDEX_EXPIRES_AT = time.time() + INDEX_TTL_SECONDS
                return True
            return False
        finally:
            REFRESH_IN_FLIGHT = False


def maybe_trigger_refresh_in_background() -> None:
    if not _index_is_stale():
        return
    try:
        asyncio.get_running_loop().create_task(refresh_index(force=True))
    except RuntimeError:
        pass


# ----------------------
# Opsiyonel: Panelden Betco config çekme
# ----------------------
def _cfg_is_stale() -> bool:
    return (not PANEL_CFG) or (time.time() >= CFG_EXPIRES_AT)


def _safe_json_loads(s: str) -> dict:
    try:
        d = json.loads(s)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _apply_panel_config(cfg: dict) -> None:
    """
    Panel config response'unu esnek okur ve betco değişkenlerini runtime'da günceller.
    Response formatın farklıysa bile çoğu ismi yakalar.
    """
    global API_BASE, API_COOKIES, API_AUTHENTICATION, API_AUTHTOKEN, EXTRA_JSON, ORIGIN, REFERER, USER_AGENT, API_LANG, APP_VERSION, PARTNER_ID, VERIFY_SSL, BETCO_TIMEOUT
    global EXTRA_HEADERS

    src = cfg
    # Sık görülen sarımlar
    if isinstance(cfg.get("data"), dict):
        src = cfg["data"]
    if isinstance(cfg.get("betco"), dict):
        src = cfg["betco"]

    # esnek key okuma
    API_BASE = (src.get("apiBase") or src.get("API_BASE") or API_BASE).strip().rstrip("/")
    if API_BASE.endswith("backofficewebadmin.betconstruct.com"):
        API_BASE = API_BASE + "/api/en"

    API_COOKIES = (src.get("cookies") or src.get("cookie") or src.get("API_COOKIES") or API_COOKIES).strip()
    API_AUTHENTICATION = (src.get("authentication") or src.get("Authentication") or src.get("API_AUTHENTICATION") or API_AUTHENTICATION).strip()
    API_AUTHTOKEN = (src.get("authToken") or src.get("authtoken") or src.get("API_AUTHTOKEN") or API_AUTHTOKEN).strip()
    ORIGIN = (src.get("origin") or src.get("ORIGIN") or ORIGIN).strip()
    REFERER = (src.get("referer") or src.get("REFERER") or REFERER).strip()
    USER_AGENT = (src.get("userAgent") or src.get("USER_AGENT") or USER_AGENT).strip()
    API_LANG = (src.get("language") or src.get("API_LANG") or API_LANG).strip() or "en"
    APP_VERSION = (src.get("appVersion") or src.get("APP_VERSION") or APP_VERSION).strip()
    PARTNER_ID = (src.get("partnerId") or src.get("PARTNER_ID") or PARTNER_ID).strip()

    # verify / timeout opsiyonel
    if "verifySsl" in src:
        VERIFY_SSL = str(src.get("verifySsl")).lower() in ("1", "true", "yes", "on")
    if "timeout" in src:
        try:
            BETCO_TIMEOUT = float(src.get("timeout"))
        except Exception:
            pass

    # extra headers json
    ej = src.get("extraHeadersJson") or src.get("EXTRA_HEADERS_JSON") or src.get("extraHeaders") or ""
    if isinstance(ej, dict):
        EXTRA_JSON = json.dumps(ej, ensure_ascii=False)
    else:
        EXTRA_JSON = str(ej).strip() if ej else EXTRA_JSON

    EXTRA_HEADERS = _safe_json_loads(EXTRA_JSON)
    # newline temizliği
    EXTRA_HEADERS = {str(k): str(v).replace("\r", "").replace("\n", " ") for k, v in EXTRA_HEADERS.items()}


async def refresh_panel_config(force: bool = False) -> bool:
    global PANEL_CFG, CFG_EXPIRES_AT

    if not PANEL_CONFIG_URL:
        return False
    if not force and not _cfg_is_stale():
        return False

    async with CFG_LOCK:
        if not force and not _cfg_is_stale():
            return False
        try:
            cfg = await asyncio.to_thread(_get_json, PANEL_CONFIG_URL, None, PANEL_TIMEOUT)
            if isinstance(cfg, dict) and (cfg.get("ok") is True or cfg.get("success") is True or cfg):
                PANEL_CFG = cfg
                CFG_EXPIRES_AT = time.time() + CONFIG_TTL_SECONDS
                _apply_panel_config(cfg)
                return True
            return False
        except Exception as e:
            if DEBUG_BETCO:
                print("[PANEL CFG] refresh failed:", repr(e))
            return False


def maybe_refresh_config_background() -> None:
    if not PANEL_CONFIG_URL:
        return
    if not _cfg_is_stale():
        return
    try:
        asyncio.get_running_loop().create_task(refresh_panel_config(force=True))
    except RuntimeError:
        pass


# ----------------------
# Member detail (rewards/history)
# ----------------------
def parse_any_date(v) -> datetime | None:
    if not v:
        return None
    if isinstance(v, datetime):
        return v
    try:
        if isinstance(v, (int, float)):
            ts = float(v)
            if ts > 1e12:
                ts = ts / 1000.0
            return datetime.fromtimestamp(ts)

        if isinstance(v, str):
            s = v.strip()
            # "2025-07-11 08:05:19.859069" veya "2025-12-22T04:41:34.054"
            for fmt in (
                "%Y-%m-%d %H:%M:%S.%f",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%S",
                "%d.%m.%Y %H:%M:%S",
                "%d.%m.%Y",
                "%d/%m/%Y %H:%M:%S",
                "%d/%m/%Y",
            ):
                try:
                    return datetime.strptime(s, fmt)
                except Exception:
                    continue
            # ISO fallback
            try:
                return datetime.fromisoformat(s.replace("Z", "+00:00"))
            except Exception:
                return None
    except Exception:
        return None
    return None


def fmt_ddmmyyyy(v) -> str:
    d = parse_any_date(v)
    return d.strftime("%d/%m/%Y") if d else "-"


def fmt_deposit_date(v) -> str:
    return fmt_ddmmyyyy(v)


def _get_member_detail_sync(member_id: int) -> dict | None:
    url = f"{PANEL_API_BASE}/api/members/{member_id}"
    j = _get_json(url, params=None, timeout=PANEL_TIMEOUT)
    if isinstance(j, dict) and j.get("ok") is True and isinstance(j.get("member"), dict):
        return j["member"]
    return None


def _latest_level_reward_from_member(member: dict | None) -> tuple[str, str]:
    """
    Returns: (Son Aldığı Seviye Ödülü, Seviye Ödül Tarihi)
    """
    if not member or not isinstance(member, dict):
        return ("-", "-")

    # history en temiz kaynak
    hist = member.get("history") or []
    best = None
    if isinstance(hist, list):
        scored = []
        for h in hist:
            if not isinstance(h, dict):
                continue
            dt = parse_any_date(h.get("rewardAt") or h.get("reward_at") or h.get("rewardDate"))
            if dt:
                scored.append((dt, h))
        if scored:
            scored.sort(key=lambda x: x[0], reverse=True)
            best = scored[0][1]

    if best:
        name = best.get("name") or VIP_TR_NAME.get(str(best.get("id")), "-") or "-"
        dt = best.get("rewardAt") or "-"
        return (str(name), fmt_ddmmyyyy(dt))

    # history yoksa rewards dict
    rewards = member.get("rewards") or {}
    if isinstance(rewards, dict):
        scored = []
        for lvl, dt_raw in rewards.items():
            dt = parse_any_date(dt_raw)
            if dt:
                scored.append((dt, lvl))
        if scored:
            scored.sort(key=lambda x: x[0], reverse=True)
            dt, lvl = scored[0]
            return (VIP_TR_NAME.get(str(lvl), str(lvl)), dt.strftime("%d/%m/%Y"))

    return ("-", "-")


# ---------- FORMAT ----------
def fmt_tl(x: int | float | None) -> str:
    if x is None:
        return "-"
    try:
        s = f"{int(float(x)):,}".replace(",", ".")
        return f"{s} TL"
    except Exception:
        return f"{x} TL"


def fmt_amount(x: int | float | None) -> str:
    if x is None:
        return "-"
    try:
        if float(x).is_integer():
            return f"{int(float(x)):,}".replace(",", ".")
        return str(x)
    except Exception:
        return str(x)


def next_level_remaining(level_id: str | None, deposit90d: int | float | None) -> tuple[str, int]:
    if not level_id or deposit90d is None:
        return ("-", 0)
    if level_id not in VIP_ORDER:
        return ("-", 0)

    idx = VIP_ORDER.index(level_id)
    if idx >= len(VIP_ORDER) - 1:
        return ("En üst seviye", 0)

    next_level = VIP_ORDER[idx + 1]
    target = VIP_TARGET_90D.get(next_level, 0)
    remaining = max(0, int(target - float(deposit90d)))
    return (next_level, remaining)


def format_panel_block(item: dict) -> str:
    level = item.get("level") or {}
    level_name = (level.get("name") if isinstance(level, dict) else None) or item.get("levelName") or "-"

    level_id = None
    if isinstance(level, dict):
        level_id = level.get("id")
    if not level_id:
        level_id = item.get("levelId")

    deposit90d_raw = item.get("deposit90d", 0)
    deposit90d = fmt_tl(deposit90d_raw)

    _, remaining_raw = next_level_remaining(str(level_id) if level_id else None, deposit90d_raw)
    remaining = fmt_tl(remaining_raw)

    # Parantez içi (PLAT) vs KALDIRILDI
    return (
        f"VIP Statü Seviyesi: {level_name}\n"
        f"Son 90 Günlük Yatırım: {deposit90d}\n"
        f"Bir Sonraki Statü Kalan: {remaining}\n"
    )


# ======================
# BETCO helpers
# ======================
def _parse_extra_headers() -> dict[str, str]:
    if not EXTRA_JSON:
        return {}
    try:
        d = json.loads(EXTRA_JSON)
        if not isinstance(d, dict):
            return {}
        return {str(k): str(v).replace("\r", "").replace("\n", " ") for k, v in d.items()}
    except Exception:
        return {}


EXTRA_HEADERS = _parse_extra_headers()


def _build_headers_base() -> dict[str, str]:
    h = {
        "Content-Type": "application/json; charset=UTF-8",
        "Accept": "application/json, text/plain, */*",
        "Origin": ORIGIN,
        "Referer": REFERER,
        "User-Agent": USER_AGENT,
        "language": API_LANG,
        "X-Requested-With": "XMLHttpRequest",
    }
    if APP_VERSION:
        h["appVersion"] = APP_VERSION
    if PARTNER_ID:
        h["partnerId"] = PARTNER_ID
    if API_COOKIES:
        h["Cookie"] = API_COOKIES

    if API_AUTHENTICATION:
        h["Authentication"] = API_AUTHENTICATION
    if API_AUTHTOKEN:
        h["authToken"] = API_AUTHTOKEN

    for k, v in (EXTRA_HEADERS or {}).items():
        if v and str(v).strip():
            h[k] = str(v).strip()

    # boşları temizle
    for k in [k for k, v in h.items() if not str(v).strip()]:
        h.pop(k, None)
    return h


def _auth_variants(base: dict[str, str]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    out.append(dict(base))

    if API_AUTHENTICATION:
        v = dict(base)
        v.pop("authToken", None)
        v["Authentication"] = API_AUTHENTICATION
        out.append(v)

    if API_AUTHTOKEN:
        v = dict(base)
        v.pop("Authentication", None)
        v["authToken"] = API_AUTHTOKEN
        out.append(v)

    if API_AUTHENTICATION and API_AUTHTOKEN:
        v = dict(base)
        v["Authentication"] = API_AUTHENTICATION
        v["authToken"] = API_AUTHTOKEN
        out.append(v)

    v = dict(base)
    v.pop("Authentication", None)
    v.pop("authToken", None)
    out.append(v)

    uniq: list[dict[str, str]] = []
    seen = set()
    for vv in out:
        key = tuple(sorted(vv.items()))
        if key not in seen:
            seen.add(key)
            uniq.append(vv)
    return uniq


async def betco_post_json(path: str, payload: dict) -> dict:
    url = f"{API_BASE}{path if path.startswith('/') else '/' + path}"
    base_headers = _build_headers_base()
    variants = _auth_variants(base_headers)

    async with httpx.AsyncClient(timeout=httpx.Timeout(BETCO_TIMEOUT), verify=VERIFY_SSL) as client:
        last_401 = None
        last_err = None

        for h in variants:
            try:
                r = await client.post(url, headers=h, json=payload)
                if r.status_code == 401:
                    last_401 = "401"
                    continue
                if not r.is_success:
                    raise RuntimeError(f"{path} HTTP {r.status_code}: {r.text[:220]}")
                return r.json()
            except Exception as e:
                last_err = e
                continue

        if last_err:
            raise last_err
        raise RuntimeError(last_401 or "Betco auth failed")


async def betco_get_json(path: str, params: dict) -> dict:
    url = f"{API_BASE}{path if path.startswith('/') else '/' + path}"
    base_headers = _build_headers_base()
    variants = _auth_variants(base_headers)

    async with httpx.AsyncClient(timeout=httpx.Timeout(BETCO_TIMEOUT), verify=VERIFY_SSL) as client:
        last_401 = None
        last_err = None

        for h in variants:
            try:
                r = await client.get(url, headers=h, params=params)
                if r.status_code == 401:
                    last_401 = "401"
                    continue
                if not r.is_success:
                    raise RuntimeError(f"{path} HTTP {r.status_code}: {r.text[:220]}")
                return r.json()
            except Exception as e:
                last_err = e
                continue

        if last_err:
            raise last_err
        raise RuntimeError(last_401 or "Betco auth failed")


async def betco_get_client_id_by_login(login: str) -> int | None:
    payload = {
        "Login": login,
        "SkeepRows": 0,
        "MaxRows": 20,
        "OrderedItem": 1,
        "IsOrderedDesc": True,
        "IsStartWithSearch": False,
        "MaxCreatedLocalDisable": True,
        "MinCreatedLocalDisable": True
    }
    j = await betco_post_json("/Client/GetClients", payload)
    objs = (j.get("Data") or {}).get("Objects") or []
    if not objs:
        return None
    cid = objs[0].get("Id")
    try:
        return int(cid)
    except Exception:
        return None


def pick_ci(d: dict, *names: str):
    if not isinstance(d, dict):
        return None
    lower_map = {str(k).lower(): k for k in d.keys()}
    for name in names:
        k = lower_map.get(name.lower())
        if k is not None:
            return d.get(k)
    return None


# ======================
# BONUS HELPERS (En son bonus)
# ======================
def _extract_bonus_objects(raw: dict | list | None) -> list[dict]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [x for x in raw if isinstance(x, dict)]
    if not isinstance(raw, dict):
        return []

    for k in ("Data", "data"):
        v = raw.get(k)
        if isinstance(v, dict):
            for kk in ("Objects", "objects", "Items", "items"):
                vv = v.get(kk)
                if isinstance(vv, list):
                    return [x for x in vv if isinstance(x, dict)]
        if isinstance(v, list):
            return [x for x in v if isinstance(x, dict)]

    for k in ("Objects", "objects", "Items", "items"):
        v = raw.get(k)
        if isinstance(v, list):
            return [x for x in v if isinstance(x, dict)]

    for k in ("WageringBonuses", "WageringBonus", "Bonuses", "Bonus", "ClientBonuses", "clientBonuses"):
        v = raw.get(k)
        if isinstance(v, list):
            return [x for x in v if isinstance(x, dict)]
        if isinstance(v, dict):
            vv = v.get("Objects") or v.get("objects")
            if isinstance(vv, list):
                return [x for x in vv if isinstance(x, dict)]

    return []


def pick_first(d: dict, keys: list[str]):
    for k in keys:
        if k in d and d[k] not in (None, "", []):
            return d[k]
    return None


def latest_bonus_from_list(bonuses: list[dict]) -> dict | None:
    if not bonuses:
        return None

    def bonus_date(b: dict) -> datetime | None:
        dt = pick_first(b, [
            "ResultDateLocal", "resultDateLocal", "ResultDate", "resultDate",
            "AcceptanceDateLocal", "acceptanceDateLocal", "AcceptanceDate", "acceptanceDate",
            "ModifiedLocal", "modifiedLocal", "ModifiedAt", "modifiedAt",
            "usedAt", "used_at", "UsedAt", "UsedAtLocal",
            "updatedAt", "updated_at", "UpdatedAt",
            "date", "Date", "bonusDate", "BonusDate",
        ])
        d = parse_any_date(dt)
        if d:
            return d

        created = pick_first(b, [
            "CreatedLocal", "createdLocal",
            "createdAt", "created_at", "CreatedAt",
            "CreateDate", "createDate"
        ])
        return parse_any_date(created)

    scored = []
    for b in bonuses:
        d = bonus_date(b)
        if d:
            scored.append((d, b))

    if not scored:
        return None

    scored.sort(key=lambda x: x[0], reverse=True)
    b = scored[0][1]

    name = pick_first(b, ["Name", "name", "BonusName", "bonusName", "title"])
    amount = pick_first(b, ["Amount", "amount", "BonusAmount", "bonusAmount", "value", "Value"])

    amt_num = None
    try:
        if amount is not None:
            s = str(amount).strip().replace(" ", "")
            if "," in s and "." in s:
                s = s.replace(".", "").replace(",", ".")
            elif "," in s and "." not in s:
                s = s.replace(",", ".")
            amt_num = float(s)
    except Exception:
        amt_num = None

    date_raw = pick_first(b, [
        "ResultDateLocal", "resultDateLocal", "ResultDate", "resultDate",
        "AcceptanceDateLocal", "acceptanceDateLocal", "AcceptanceDate", "acceptanceDate",
        "ModifiedLocal", "modifiedLocal", "ModifiedAt", "modifiedAt",
        "usedAt", "used_at", "UsedAt", "UsedAtLocal",
        "updatedAt", "updated_at", "UpdatedAt",
        "date", "Date", "bonusDate", "BonusDate",
        "CreatedLocal", "createdLocal",
        "createdAt", "created_at", "CreatedAt", "CreateDate", "createDate",
    ])

    return {"name": str(name) if name is not None else None, "amount": amt_num, "date_raw": date_raw}


async def betco_fetch_latest_bonus_by_client_id(client_id: int) -> dict | None:
    candidates = [
        ("GET",  "/Bonus/GetClientBonuses", {"id": client_id}, None),
        ("GET",  "/Client/GetClientBonuses", {"id": client_id}, None),
        ("GET",  "/Bonus/GetWageringBonuses", {"clientId": client_id}, None),
        ("POST", "/Bonus/GetClientBonuses", None, {"ClientId": client_id, "SkeepRows": 0, "MaxRows": 50}),
        ("POST", "/Client/GetClientBonuses", None, {"ClientId": client_id, "SkeepRows": 0, "MaxRows": 50}),
    ]

    for method, path, params, payload in candidates:
        try:
            raw = await (betco_get_json(path, params or {}) if method == "GET" else betco_post_json(path, payload or {}))
            if isinstance(raw, dict) and raw.get("HasError") is True:
                continue
            objs = _extract_bonus_objects(raw)
            latest = latest_bonus_from_list(objs)
            if latest:
                return latest
        except Exception:
            continue

    return None


async def betco_fetch_kpi_by_login(login: str) -> dict:
    now = time.time()
    cached = BETCO_CACHE.get(login)
    if cached and now < cached[0]:
        return cached[1]

    client_id = await betco_get_client_id_by_login(login)
    if not client_id:
        out = {
            "status": "not_found",
            "clientId": None,
            "lastDepositAmount": None,
            "lastDepositTime": None,
            "latestBonusName": None,
            "latestBonusAmount": None,
            "latestBonusDate": None,
        }
        BETCO_CACHE[login] = (time.time() + BETCO_CACHE_TTL, out)
        return out

    bonus_task = asyncio.create_task(betco_fetch_latest_bonus_by_client_id(client_id))
    raw = await betco_get_json("/Client/GetClientKpi", {"id": client_id})

    kpi = raw["Data"] if isinstance(raw, dict) and isinstance(raw.get("Data"), dict) else raw

    if isinstance(raw, dict) and raw.get("HasError") is True:
        msg = raw.get("AlertMessage") or "KPI HasError"
        out = {
            "status": "error",
            "clientId": client_id,
            "lastDepositAmount": None,
            "lastDepositTime": None,
            "latestBonusName": None,
            "latestBonusAmount": None,
            "latestBonusDate": None,
            "message": msg,
        }
        BETCO_CACHE[login] = (time.time() + BETCO_CACHE_TTL, out)
        return out

    last_amt = pick_ci(kpi, "LastDepositAmount", "DepositAmount", "TotalDeposit")
    last_time = pick_ci(kpi, "LastDepositTimeLocal", "LastDepositTime", "FirstDepositTimeLocal", "FirstDepositTime")

    try:
        if last_amt is not None:
            last_amt = float(last_amt)
    except Exception:
        pass

    latest_bonus = None
    try:
        latest_bonus = await asyncio.wait_for(bonus_task, timeout=BETCO_TIMEOUT)
    except Exception:
        latest_bonus = None

    out = {
        "status": "OK",
        "clientId": client_id,
        "lastDepositAmount": last_amt,
        "lastDepositTime": last_time,
        "latestBonusName": (latest_bonus or {}).get("name") if latest_bonus else None,
        "latestBonusAmount": (latest_bonus or {}).get("amount") if latest_bonus else None,
        "latestBonusDate": fmt_ddmmyyyy((latest_bonus or {}).get("date_raw")) if latest_bonus else None,
    }
    BETCO_CACHE[login] = (time.time() + BETCO_CACHE_TTL, out)
    return out


# ======================
# Output formatter (temiz mesaj)
# ======================
def build_final_message(username: str, panel_block: str, b: dict | None, reward_name: str, reward_date: str) -> str:
    # Betco kısmı yok (başlık kaldırıldı), nokta/bullet yok
    if not b or b.get("status") != "OK":
        dep_amt = "-"
        dep_at = "-"
        bonus_name = "-"
        bonus_amt = "-"
        bonus_date = "-"
    else:
        dep_amt = fmt_tl(b.get("lastDepositAmount")) if b.get("lastDepositAmount") is not None else "-"
        dep_at = fmt_deposit_date(b.get("lastDepositTime"))
        bonus_name = b.get("latestBonusName") or "-"
        bonus_amt = fmt_amount(b.get("latestBonusAmount")) if b.get("latestBonusAmount") is not None else "-"
        bonus_date = b.get("latestBonusDate") or "-"

    return (
        f"Kullanıcı Adı: {username}\n\n"
        f"{panel_block}\n"
        f"Son Yatırım Miktarı: {dep_amt}\n"
        f"Son Yatırım Tarihi: {dep_at}\n"
        f"Bonus Adı: {bonus_name}\n"
        f"Bonus Miktarı: {bonus_amt}\n"
        f"Bonus Tarihi: {bonus_date}\n"
        f"Son Aldığı Seviye Ödülü: {reward_name}\n"
        f"Seviye Ödül Tarihi: {reward_date}\n"
    )


# ======================
# Telegram handlers
# ======================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_allowed(update):
        await update.message.reply_text("⛔ Yetkin yok.")
        return

    # config + index arkaplanda tazelensin
    maybe_refresh_config_background()
    maybe_trigger_refresh_in_background()

    await update.message.reply_text(
        "✅ Bot çalışıyor.\n"
    )

async def chatid(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    user = update.effective_user
    if chat is None or update.message is None:
        return

    await update.message.reply_text(
        f"chat_id: {chat.id}\n"
        f"chat_type: {chat.type}\n"
        f"user_id: {user.id if user else '-'}"
    )


async def selftest(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_allowed(update):
        await update.message.reply_text("⛔ Yetkin yok.")
        return

    # config çekmeyi dene (varsa)
    await refresh_panel_config(force=False)

    try:
        _ = await betco_post_json("/Client/GetClients", {
            "Login": "probe_user",
            "SkeepRows": 0,
            "MaxRows": 1,
            "OrderedItem": 1,
            "IsOrderedDesc": True,
            "IsStartWithSearch": False,
            "MaxCreatedLocalDisable": True,
            "MinCreatedLocalDisable": True
        })
        await update.message.reply_text("✅ Betco selftest OK (GetClients erişilebilir).")
    except Exception as e:
        await update.message.reply_text(f"❌ Betco selftest FAIL: {repr(e)}")


async def ka(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_allowed(update):
        await update.message.reply_text("⛔ Yetkin yok.")
        return
    if not context.args:
        await update.message.reply_text("Kullanım: /ka username")
        return

    username = context.args[0].strip()

    # Panel config (opsiyonel) + index
    await refresh_panel_config(force=False)

    if not USER_INDEX:
        await update.message.reply_text("🔄 İlk indeks hazırlanıyor...")
        ok = await refresh_index(force=True)
        if not ok or not USER_INDEX:
            await update.message.reply_text("⚠️ Panelden indeks alınamadı. Tekrar dene.")
            return

    maybe_refresh_config_background()
    maybe_trigger_refresh_in_background()

    item = USER_INDEX.get(username)
    if not item:
        await update.message.reply_text(f"❌ Bulunamadı: {username}")
        return

    panel_block = format_panel_block(item)

    # Member detail ile reward bilgisi
    member_id = item.get("id")
    reward_name, reward_date = "-", "-"
    if isinstance(member_id, int):
        try:
            member = await asyncio.to_thread(_get_member_detail_sync, member_id)
            reward_name, reward_date = _latest_level_reward_from_member(member)
        except Exception:
            reward_name, reward_date = "-", "-"

    betco_task = asyncio.create_task(betco_fetch_kpi_by_login(username))

    # “Sorgulanıyor” yerine
    msg = await update.message.reply_text(
        f"Kullanıcı Adı: {username}\n\n{panel_block}\nYatırım hesaplanıyor..."
    )

    try:
        b = await asyncio.wait_for(betco_task, timeout=BETCO_TIMEOUT + 2)
        final_text = build_final_message(username, panel_block, b, reward_name, reward_date)
        try:
            await msg.edit_text(final_text)
        except Exception:
            await update.message.reply_text(final_text)
    except Exception as e:
        print("\n[BETCO ERROR]", repr(e))
        print(traceback.format_exc())
        final_text = build_final_message(username, panel_block, None, reward_name, reward_date)
        try:
            await msg.edit_text(final_text)
        except Exception:
            await update.message.reply_text(final_text)


def main() -> None:
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("chatid", chatid))  # en üstte dursun
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("selftest", selftest))
    app.add_handler(CommandHandler("ka", ka))

    # job_queue opsiyonel
    if app.job_queue:
        async def refresh_index_job(context: ContextTypes.DEFAULT_TYPE) -> None:
            await refresh_index(force=True)

        async def refresh_cfg_job(context: ContextTypes.DEFAULT_TYPE) -> None:
            await refresh_panel_config(force=True)

        app.job_queue.run_once(refresh_index_job, when=1)
        app.job_queue.run_repeating(refresh_index_job, interval=INDEX_TTL_SECONDS, first=INDEX_TTL_SECONDS)

        if PANEL_CONFIG_URL:
            app.job_queue.run_once(refresh_cfg_job, when=2)
            app.job_queue.run_repeating(refresh_cfg_job, interval=CONFIG_TTL_SECONDS, first=CONFIG_TTL_SECONDS)

    print("Bot başladı. Telegram’dan /start yaz.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()