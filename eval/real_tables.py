"""Hand-made sample tables for the real-world queries in eval/real_queries.jsonl.

These tables (raw_events, marketing_campaigns, fraud_blacklist) are not in
sql2pyspark-mlx/schemas.py, so the dataset generator never sees them. The rows are
chosen so every clause of the queries changes the result: duplicate events, NULL
user ids and amounts, pre-2025 events, 30-minute session gaps, campaign windows
(+7 days), users with one session, >10 events per user, and fraud flags before and
after a user's events. Event times are unique per user (apart from exact duplicates
that dedup removes), so window results do not depend on how ties are broken.

Used by:  python eval/evaluate.py --tables eval/real_tables.py --set eval/real_queries.jsonl ...
"""
from __future__ import annotations

from datetime import date, datetime

RAW_EVENTS_DDL = ("user_id INT, session_id STRING, event_time TIMESTAMP, event_type STRING, "
                  "amount DOUBLE, channel STRING")
CAMPAIGNS_DDL = ("campaign_id STRING, campaign_type STRING, channel_code STRING, "
                 "start_time TIMESTAMP, end_time TIMESTAMP")
BLACKLIST_DDL = "user_id INT, flagged_date DATE"


def _ts(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d %H:%M")


RAW_EVENTS = [
    # user 1, EMAIL: 3 sessions, 12 distinct events (spend_rank drops the 2 cheapest), duplicates at 10:20
    (1, "s1a", "2024-12-30 12:00", "PURCHASE", 999.0, "EMAIL"),        # before 2025 -> filtered out
    (1, "s1a", "2025-01-05 10:00", "VIEW", 5.0, "EMAIL"),
    (1, "s1a", "2025-01-05 10:10", "ADD_TO_CART", 12.5, "EMAIL"),
    (1, "s1a", "2025-01-05 10:20", "PURCHASE", 120.0, "EMAIL"),
    (1, "s1a", "2025-01-05 10:20", "PURCHASE", 80.0, "EMAIL"),         # duplicate, lower amount
    (1, "s1a", "2025-01-05 10:20", "PURCHASE", None, "EMAIL"),         # duplicate, NULL amount
    (1, "s1b", "2025-02-10 09:00", "VIEW", 3.0, "EMAIL"),
    (1, "s1b", "2025-02-10 09:25", "PURCHASE", 45.0, "EMAIL"),
    (1, "s1b", "2025-02-10 10:30", "VIEW", 7.0, "EMAIL"),              # 65-min gap -> new session
    (1, "s1c", "2025-03-03 18:00", "VIEW", 2.0, "EMAIL"),
    (1, "s1c", "2025-03-03 18:05", "ADD_TO_CART", 9.99, "EMAIL"),
    (1, "s1c", "2025-03-03 18:15", "PURCHASE", 210.0, "EMAIL"),
    (1, "s1c", "2025-03-03 18:40", "PURCHASE", 15.0, "EMAIL"),
    (1, "s1c", "2025-03-03 18:50", "VIEW", 1.0, "EMAIL"),
    (1, "s1c", "2025-03-03 19:00", "VIEW", 0.5, "EMAIL"),
    # user 2, channel NULL -> DIRECT: 2 sessions, flagged as fraud on 2025-02-15
    (2, "s2a", "2025-01-12 08:00", "VIEW", 4.0, None),
    (2, "s2a", "2025-01-12 08:10", "PURCHASE", 60.0, None),
    (2, "s2b", "2025-02-20 20:00", "PURCHASE", 75.0, None),
    (2, "s2b", "2025-02-20 20:05", "VIEW", 1.5, None),
    # user 3, SOCIAL: a single session -> removed by HAVING
    (3, "s3a", "2025-01-20 14:00", "VIEW", 2.0, "SOCIAL"),
    (3, "s3a", "2025-01-20 14:10", "PURCHASE", 30.0, "SOCIAL"),
    # user 4, SOCIAL: campaign ended in 2024 -> ORGANIC; NULL amounts
    (4, "s4a", "2025-01-25 11:00", "VIEW", None, "SOCIAL"),
    (4, "s4a", "2025-01-25 11:05", "PURCHASE", 55.0, "SOCIAL"),
    (4, "s4b", "2025-03-10 16:00", "PURCHASE", 88.0, "SOCIAL"),
    (4, "s4b", "2025-03-10 16:20", "VIEW", None, "SOCIAL"),
    # user 5, SEARCH: first session inside campaign end + 7 days, second after it; flagged only in 2026
    (5, "s5a", "2025-02-05 07:00", "PURCHASE", 150.0, "SEARCH"),
    (5, "s5a", "2025-02-05 07:15", "VIEW", 6.0, "SEARCH"),
    (5, "s5b", "2025-02-10 22:00", "PURCHASE", 40.0, "SEARCH"),
    # no user -> filtered out
    (None, "sx", "2025-01-15 12:00", "PURCHASE", 500.0, "EMAIL"),
]

CAMPAIGNS = [
    ("C-EMAIL-JAN", "NEWSLETTER", "EMAIL", "2025-01-01 00:00", "2025-01-20 00:00"),
    ("C-EMAIL-MAR", "NEWSLETTER", "EMAIL", "2025-03-01 00:00", "2025-03-15 00:00"),
    ("C-DIRECT-Q1", "ALWAYS_ON", "DIRECT", "2025-01-01 00:00", "2025-03-31 00:00"),
    ("C-SOCIAL-24", "PAID_SOCIAL", "SOCIAL", "2024-06-01 00:00", "2024-06-30 00:00"),
    ("C-SEARCH-JAN", "PPC", "SEARCH", "2025-01-10 00:00", "2025-01-31 00:00"),
]

BLACKLIST = [
    (2, date(2025, 2, 15)),
    (5, date(2026, 1, 1)),
]


def make_dataframes(spark) -> dict:
    """Return {"df_<table>": DataFrame} and register each table as a temp view."""
    data = {
        "raw_events": ([(u, s, _ts(t), e, a, c) for u, s, t, e, a, c in RAW_EVENTS], RAW_EVENTS_DDL),
        "marketing_campaigns": ([(i, ty, ch, _ts(a), _ts(b)) for i, ty, ch, a, b in CAMPAIGNS], CAMPAIGNS_DDL),
        "fraud_blacklist": (BLACKLIST, BLACKLIST_DDL),
    }
    frames = {}
    for name, (rows, ddl) in data.items():
        df = spark.createDataFrame(rows, ddl)
        df.createOrReplaceTempView(name)
        frames[f"df_{name}"] = df
    return frames
