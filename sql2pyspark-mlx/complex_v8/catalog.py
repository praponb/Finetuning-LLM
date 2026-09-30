"""Table catalog for the complex_v8 dataset (standalone: does not import or change schemas.py).

Six new domains (music streaming, insurance, warehouse logistics, energy metering, education, customer
support) with 20 tables that do NOT appear in the v1-v7 training data or in any eval set, so the model
learns the SQL -> PySpark mapping rather than table names.

Used by generate.py (column metadata + real sample values for filter thresholds) and verify.py
(seeded DataFrames `df_<table>` + temp views for the execution check).
"""
from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta

SPARK_TYPES = {"id": "int", "fk": "int", "int": "int", "double": "double", "cat": "string", "str": "string",
               "date": "date", "ts": "timestamp", "bool": "boolean", "array": "array<string>"}


@dataclass(frozen=True)
class Col:
    name: str
    kind: str                 # id fk int double cat str date ts bool array
    pool: tuple = ()          # values for cat / str / array
    lo: float = 0
    hi: float = 100
    ref: str | None = None    # table whose id this column holds (id / fk)
    nullable: bool = True
    src: tuple | None = None  # (table, column) this column was copied from (for data-aware thresholds)


@dataclass(frozen=True)
class Table:
    name: str
    domain: str
    columns: tuple
    n_rows: int = 40

    def col(self, name: str) -> Col:
        return next(c for c in self.columns if c.name == name)

    @property
    def ddl(self) -> str:
        return ", ".join(f"{c.name} {SPARK_TYPES[c.kind]}" for c in self.columns)


def _id(table: str) -> Col:
    return Col("id", "id", ref=table, nullable=False)


def _fk(name: str, ref: str, hi: int, nullable: bool = True) -> Col:
    return Col(name, "fk", lo=1, hi=hi, ref=ref, nullable=nullable)


def _cat(name: str, *pool: str, nullable: bool = True) -> Col:
    return Col(name, "cat", pool=pool, nullable=nullable)


def _str(name: str, *pool: str) -> Col:
    return Col(name, "str", pool=pool)


def _num(name: str, lo: float, hi: float, kind: str = "double", nullable: bool = True) -> Col:
    return Col(name, kind, lo=lo, hi=hi, nullable=nullable)


def _date(name: str, nullable: bool = True) -> Col:
    return Col(name, "date", nullable=nullable)


def _ts(name: str, nullable: bool = True) -> Col:
    return Col(name, "ts", nullable=nullable)


def _bool(name: str) -> Col:
    return Col(name, "bool")


def _arr(name: str, *pool: str) -> Col:
    return Col(name, "array", pool=pool, nullable=False)


PEOPLE = ("Anong Chai", "ben carter", "  Chloe Martin", "Dara Kim ", "ELLIS GRANT", "Fah Suwan", "Gus Novak",
          "Hana Sato", "Iris Moreau", "Jon Ruiz", "Kit Wong", "Lena Fischer", "Milo Hart", "Nok Prasert")
COUNTRIES = ("TH", "US", "KR", "JP", "UK", "BR")

TABLES: tuple[Table, ...] = (
    # ---- music streaming ----
    Table("artists", "music", (
        _id("artists"), _str("artist_name", "Nova Lights", "the echoes", "Siam Groove", "  Blue Harbor",
                             "KITE", "Luna Park ", "Paper Tigers", "Mango Beat"),
        _cat("genre", "Pop", "Rock", "Jazz", "HipHop", "Classical", "Indie", nullable=False),
        _cat("country", *COUNTRIES), _num("debut_year", 1990, 2023, "int"), _bool("is_verified"),
    ), 15),
    Table("tracks", "music", (
        _id("tracks"), _fk("artist_id", "artists", 15, nullable=False),
        _str("title", "Midnight Drive", "paper heart", "Golden Hour", "  Rain Song", "NEON", "Slow Burn",
             "City Lights", "Echo Chamber"),
        _num("duration_sec", 90, 420, "int", nullable=False), _date("release_date"), _bool("is_explicit"),
        _num("bpm", 60, 180, "int"), _arr("moods", "chill", "party", "focus", "sad", "workout"),
    ), 30),
    Table("listeners", "music", (
        _id("listeners"), _str("username", "anong_c", "Ben.C", "chloe_m", "dara99", "ellis", "fahfah", "gus_n",
                               "hana.s"),
        _cat("plan", "free", "premium", "family", "student", nullable=False), _cat("country", *COUNTRIES),
        _date("signup_date"), _num("birth_year", 1960, 2008, "int"),
        _arr("interests", "podcasts", "live", "indie", "kpop", "jazz"),
    ), 20),
    Table("plays", "music", (
        _id("plays"), _fk("listener_id", "listeners", 20, nullable=False), _fk("track_id", "tracks", 30),
        _ts("played_at", nullable=False), _num("ms_played", 5000, 400000, "int", nullable=False),
        _cat("device", "phone", "desktop", "speaker", "car", "tv"), _bool("is_skipped"),
        _cat("source", "playlist", "album", "radio", "search", nullable=False),
    ), 90),
    # ---- insurance ----
    Table("policyholders", "insurance", (
        _id("policyholders"), _str("holder_name", *PEOPLE),
        _cat("region", "North", "South", "East", "West", "Central", nullable=False),
        _num("risk_score", 0.01, 0.99), _date("joined_on"), _cat("segment", "RETAIL", "SME", "VIP"),
    ), 20),
    Table("adjusters", "insurance", (
        _id("adjusters"), _str("adjuster_name", *PEOPLE[:8]),
        _cat("team", "Motor", "Property", "Health", "Travel", nullable=False), _num("level", 1, 5, "int"),
    ), 8),
    Table("policies", "insurance", (
        _id("policies"), _fk("holder_id", "policyholders", 20),
        _cat("product", "AUTO", "HOME", "TRAVEL", "HEALTH", "LIFE", nullable=False),
        _num("premium", 200, 5000, nullable=False), _date("start_date", nullable=False), _date("end_date"),
        _cat("status", "ACTIVE", "LAPSED", "CANCELLED", "EXPIRED", nullable=False),
    ), 30),
    Table("claims", "insurance", (
        _id("claims"), _fk("policy_id", "policies", 30, nullable=False), _fk("adjuster_id", "adjusters", 8),
        _date("claim_date", nullable=False),
        _cat("claim_type", "collision", "theft", "fire", "flood", "medical", "delay"),
        _num("claimed_amount", 100, 50000, nullable=False), _num("approved_amount", 0, 50000),
        _cat("status", "OPEN", "APPROVED", "REJECTED", "PAID", "IN_REVIEW", nullable=False),
        _ts("reported_at", nullable=False),
    ), 80),
    # ---- warehouse logistics ----
    Table("warehouses", "logistics", (
        _id("warehouses"), _str("wh_code", "BKK1", "BKK2", "CNX1", "HKT1", "SGP1", "KUL1"),
        _cat("city", "Bangkok", "Chiang Mai", "Phuket", "Singapore", "Kuala Lumpur", nullable=False),
        _num("capacity", 1000, 20000, "int"), _date("opened_on"),
    ), 6),
    Table("skus", "logistics", (
        _id("skus"), _str("sku_code", "EL-100", "ap-220", "FD-310", " TY-405", "BT-550", "EL-777", "ap-808"),
        _cat("category", "Electronics", "Apparel", "Food", "Toys", "Beauty", nullable=False),
        _num("unit_cost", 1, 400, nullable=False), _num("weight_kg", 0.1, 25),
        _arr("labels", "fragile", "cold", "hazmat", "bulky", "express"), _bool("is_active"),
    ), 20),
    Table("stock_moves", "logistics", (
        _id("stock_moves"), _fk("warehouse_id", "warehouses", 6, nullable=False),
        _fk("sku_id", "skus", 20, nullable=False),
        _cat("move_type", "INBOUND", "OUTBOUND", "TRANSFER", "ADJUST", nullable=False),
        _num("qty", 1, 500, "int", nullable=False), _ts("moved_at", nullable=False),
        _cat("operator", "op_ann", "op_bee", "op_chan", "op_dew", "op_eve"), _num("unit_price", 1, 500),
    ), 90),
    # ---- energy metering ----
    Table("sites", "energy", (
        _id("sites"), _str("site_name", "Solar Farm A", "rooftop b", "Depot North", " Mall East", "HQ",
                           "Factory 7"),
        _cat("zone", "Z1", "Z2", "Z3", "Z4", nullable=False), _cat("tariff", "TOU", "FLAT", "EV", nullable=False),
        _num("installed_kw", 5, 500), _date("commissioned_on"),
    ), 10),
    Table("meter_readings", "energy", (
        _id("meter_readings"), _fk("site_id", "sites", 10, nullable=False), _ts("read_at", nullable=False),
        _num("kwh", 0, 120), _num("voltage", 210, 245), _cat("quality", "OK", "EST", "BAD", nullable=False),
    ), 90),
    Table("tariff_windows", "energy", (
        _id("tariff_windows"), _cat("tariff", "TOU", "FLAT", "EV", nullable=False),
        _ts("valid_from", nullable=False), _ts("valid_to", nullable=False), _num("rate", 2, 8, nullable=False),
    ), 12),
    # ---- education ----
    Table("students", "education", (
        _id("students"), _str("student_name", *PEOPLE),
        _cat("cohort", "C2021", "C2022", "C2023", "C2024", nullable=False),
        _cat("major", "CS", "Math", "Biz", "Art", "Bio"), _date("enrolled_on"), _num("gpa", 1.5, 4.0),
        _bool("is_international"),
    ), 20),
    Table("courses", "education", (
        _id("courses"), _str("course_code", "CS101", "cs201", "MA110", "BZ220", " AR105", "BI300", "CS350"),
        _cat("dept", "CS", "Math", "Biz", "Art", "Bio", nullable=False), _num("credits", 1, 4, "int", nullable=False),
        _cat("level", "intro", "core", "advanced"),
    ), 12),
    Table("enrollments", "education", (
        _id("enrollments"), _fk("student_id", "students", 20, nullable=False),
        _fk("course_id", "courses", 12, nullable=False),
        _cat("term", "2024T1", "2024T2", "2025T1", "2025T2", nullable=False), _num("score", 0, 100),
        _cat("grade_status", "PASS", "FAIL", "WITHDRAWN", "INCOMPLETE", nullable=False),
        _ts("enrolled_at", nullable=False),
    ), 90),
    # ---- customer support ----
    Table("support_agents", "support", (
        _id("support_agents"), _str("agent_name", *PEOPLE[3:12]),
        _cat("team", "Billing", "Tech", "Onboarding", "VIP", nullable=False), _cat("tier", "T1", "T2", "T3"),
        _date("hired_on"),
    ), 10),
    Table("tickets", "support", (
        _id("tickets"), _num("requester_id", 1, 25, "int", nullable=False), _fk("agent_id", "support_agents", 10),
        _ts("opened_at", nullable=False), _ts("closed_at"),
        _cat("priority", "P1", "P2", "P3", "P4", nullable=False), _cat("channel", "email", "chat", "phone", "web"),
        _num("csat", 1, 5, "int"), _arr("tags", "refund", "bug", "login", "upgrade", "outage"),
    ), 60),
    Table("ticket_events", "support", (
        _id("ticket_events"), _fk("ticket_id", "tickets", 60, nullable=False), _ts("event_at", nullable=False),
        _cat("event_type", "created", "assigned", "replied", "escalated", "resolved", "reopened", nullable=False),
        _cat("actor", "agent", "customer", "system"),
    ), 90),
)

TABLE_BY_NAME = {t.name: t for t in TABLES}
DOMAINS = sorted({t.domain for t in TABLES})

# readable prefix for columns copied from a table (artist_genre, policy_status, ...)
SINGULAR = {"artists": "artist", "tracks": "track", "listeners": "listener", "plays": "play",
            "policyholders": "holder", "adjusters": "adjuster", "policies": "policy", "claims": "claim",
            "warehouses": "warehouse", "skus": "sku", "stock_moves": "move", "sites": "site",
            "meter_readings": "reading", "tariff_windows": "tariff", "students": "student", "courses": "course",
            "enrollments": "enrollment", "support_agents": "agent", "tickets": "ticket",
            "ticket_events": "event"}


def children_of(table: str) -> list[tuple[Table, Col]]:
    """(child table, fk column) pairs whose fk references `table`."""
    return [(t, c) for t in TABLES for c in t.columns if c.kind == "fk" and c.ref == table]


# --------------------------------------------------------------------------
# Seeded sample rows
# --------------------------------------------------------------------------
DATE_LO = date(2024, 1, 1)
DATE_SPAN = 730
TS_LO = datetime(2025, 1, 1)
TS_ANCHORS = 14          # clustered timestamps: gaps of minutes inside a burst, days between bursts
TS_SPAN_DAYS = 90


def _ts_value(rng: random.Random) -> datetime:
    anchor = TS_LO + timedelta(days=rng.randint(0, TS_ANCHORS - 1) * (TS_SPAN_DAYS // TS_ANCHORS),
                               hours=rng.randint(6, 12))
    return anchor + timedelta(minutes=rng.randint(0, 240), seconds=rng.choice([0, 0, 15, 30, 45]))


def _value(t: Table, c: Col, rng: random.Random, row_id: int):
    if c.kind == "id":
        return row_id
    if c.nullable and rng.random() < 0.07:
        return None
    if c.kind == "fk":
        hi = int(c.hi)
        return rng.randint(1, hi) if rng.random() < 0.93 else rng.randint(hi + 1, hi + 3)
    if c.kind == "int":
        return rng.randint(int(c.lo), int(c.hi))
    if c.kind == "double":
        return round(rng.uniform(c.lo, c.hi), 2)
    if c.kind in ("cat", "str"):
        return rng.choice(c.pool)
    if c.kind == "date":
        return DATE_LO + timedelta(days=rng.randint(0, DATE_SPAN))
    if c.kind == "ts":
        return _ts_value(rng)
    if c.kind == "bool":
        return rng.random() < 0.5
    if c.kind == "array":
        return rng.sample(c.pool, rng.randint(0, 3))
    raise ValueError(c.kind)


def _tariff_rows(rng: random.Random) -> list[tuple]:
    rows, i = [], 1
    for tariff in ("TOU", "FLAT", "EV"):
        start = TS_LO - timedelta(days=2)
        for _ in range(4):
            end = start + timedelta(days=rng.randint(18, 26), hours=rng.randint(0, 23))
            rows.append((i, tariff, start, end, round(rng.uniform(2, 8), 2)))
            start = end + timedelta(hours=rng.choice([0, 0, 6]))   # small gaps -> some readings match no window
            i += 1
    return rows


def make_rows(t: Table, seed: int = 11) -> list[tuple]:
    rng = random.Random(f"{seed}-{t.name}")
    if t.name == "tariff_windows":
        return _tariff_rows(rng)
    rows = []
    names = [c.name for c in t.columns]
    for i in range(1, t.n_rows + 1):
        row = [_value(t, c, rng, i) for c in t.columns]
        rec = dict(zip(names, row))
        # keep the obvious invariants of the domain
        if t.name == "policies" and rec["end_date"] is not None:
            rec["end_date"] = rec["start_date"] + timedelta(days=rng.randint(90, 720))
        if t.name == "claims" and rec["approved_amount"] is not None:
            rec["approved_amount"] = round(rec["claimed_amount"] * rng.choice([0.0, 0.5, 0.8, 1.0]), 2)
        if t.name == "tickets" and rec["closed_at"] is not None:
            rec["closed_at"] = rec["opened_at"] + timedelta(minutes=rng.randint(10, 5 * 24 * 60))
        rows.append(tuple(rec[n] for n in names))
    return rows


_ROWS: dict[str, list[tuple]] = {}


def rows_of(table: str) -> list[tuple]:
    if table not in _ROWS:
        _ROWS[table] = make_rows(TABLE_BY_NAME[table])
    return _ROWS[table]


def values_of(table: str, column: str) -> list:
    t = TABLE_BY_NAME[table]
    idx = [c.name for c in t.columns].index(column)
    return [r[idx] for r in rows_of(table) if r[idx] is not None]


def make_dataframes(spark) -> dict:
    """{"df_<table>": DataFrame} for every table, each also registered as a temp view."""
    frames = {}
    for t in TABLES:
        df = spark.createDataFrame(make_rows(t), t.ddl)
        df.createOrReplaceTempView(t.name)
        frames[f"df_{t.name}"] = df
    return frames
