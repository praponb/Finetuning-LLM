"""Hand-made sample tables for the real-world queries in eval/real_queries.jsonl.

None of these tables are in sql2pyspark-mlx/schemas.py (`orders` exists there with a different
schema; the version here replaces it only when these tables are loaded), so the dataset generator
never sees them. For every query, the rows are chosen so each clause changes the result, and so
that window results and ORDER BY checks don't depend on how ties are broken.

  real001  raw_events, marketing_campaigns, fraud_blacklist
  real002  orders (nested structs/arrays), warehouse_inventory, blacklisted_fulfillments, warehouse_sla
  real003  sensor_telemetry (JSON payloads), calibration_registry, operational_incidents, device_metadata
  real004  trade_stream (maps and arrays), cleared_settlement_batches

Used by:  python eval/evaluate.py --tables eval/real_tables.py --set eval/real_queries.jsonl ...
"""
from __future__ import annotations

from datetime import date, datetime, timezone

RAW_EVENTS_DDL = ("user_id INT, session_id STRING, event_time TIMESTAMP, event_type STRING, "
                  "amount DOUBLE, channel STRING")
CAMPAIGNS_DDL = ("campaign_id STRING, campaign_type STRING, channel_code STRING, "
                 "start_time TIMESTAMP, end_time TIMESTAMP")
BLACKLIST_DDL = "user_id INT, flagged_date DATE"


def _ts(s: str) -> datetime:
    # UTC-aware, so the stored instants don't depend on the machine's time zone (the checker's session is UTC)
    return datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)


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


# ---- real002: order fulfillment (structs, arrays of structs, LATERAL VIEW EXPLODE) ----
ORDERS_DDL = ("order_id INT, customer_id INT, order_timestamp TIMESTAMP, "
              "shipping_address STRUCT<country: STRING, postal_code: STRING>, "
              "line_items ARRAY<STRUCT<sku: STRING, quantity: INT, unit_price: DOUBLE, "
              "applied_discounts: ARRAY<STRUCT<discount_code: STRING, amount: DOUBLE>>>>, "
              "eligible_warehouses ARRAY<STRUCT<warehouse_id: STRING, distance_km: DOUBLE>>, "
              "fulfillment_flags ARRAY<STRING>")
INVENTORY_DDL = "sku STRING, warehouse_id STRING, origin_country STRING, batch_id STRING, acquisition_cost DOUBLE"
BLACKLISTED_DDL = "order_id INT, sku STRING, warehouse_id STRING"
SLA_DDL = "warehouse_id STRING, dispatch_threshold_hours INT"


def _item(sku, qty, price, *discounts):
    return (sku, qty, price, [tuple(d) for d in discounts])


ORDERS = [
    (1, 11, "2026-01-03 10:00", ("US", "10001"),
     [_item("SKU-A", 12, 20.0, ("D10", 2.0), ("D0", 0.0)), _item("SKU-B", 3, 50.0)],
     [("W1", 120.0), ("W2", 300.0)], ["PRIORITY", "GIFT"]),
    (2, 12, "2026-01-05 09:00", ("US", "94105"), [_item("SKU-A", 2, 22.0, ("D5", 1.0))],
     [("W1", 80.0), ("W3", 40.0)], ["PRIORITY"]),
    (3, 13, "2026-01-06 12:00", ("DE", "10115"), [_item("SKU-C", 15, 8.0), _item("SKU-A", 1, 25.0)],
     [("W2", 900.0)], ["PRIORITY"]),
    (4, 14, "2026-01-07 08:00", (None, "00000"), [_item("SKU-B", 4, 45.0)],       # NULL country: <=> match
     [("W1", 10.0)], ["PRIORITY"]),
    (5, 15, "2026-01-08 15:00", ("BR", "01000"), [],                             # no items: OUTER explode
     [("W3", 5000.0)], ["PRIORITY"]),
    (6, 16, "2025-12-20 09:00", ("US", "10001"), [_item("SKU-A", 50, 20.0)],     # before 2026
     [("W1", 120.0)], ["PRIORITY"]),
    (7, 17, "2026-01-09 11:00", ("CA", "M5V"), [_item("SKU-D", 10, 30.0, ("D3", 3.0))],
     [("W2", 450.0)], ["PRIORITY"]),
    (8, 18, "2026-01-09 16:00", ("US", "60601"), [_item("SKU-A", 30, 19.0)],     # not PRIORITY
     [("W1", 60.0)], ["STANDARD"]),
    (9, 19, "2026-01-10 13:00", ("MX", "06600"), [_item("SKU-B", 1, 55.0)],      # blacklisted
     [("W3", 700.0)], ["PRIORITY"]),
    (10, 20, "2026-01-11 14:00", ("IT", "00184"), [_item("SKU-C", 20, 9.0), _item("SKU-E", 2, 12.0)],
     [("W2", 1100.0), ("W1", 1300.0)], ["PRIORITY"]),
]

INVENTORY = [
    ("SKU-A", "W1", "US", "B01", 12.0), ("SKU-A", "W1", "DE", "B02", 14.0), ("SKU-A", "W2", "US", "B03", 11.0),
    ("SKU-A", "W2", "DE", "B04", 13.5), ("SKU-A", "W3", "US", "B05", 15.0),
    ("SKU-B", "W1", "US", "B06", 30.0), ("SKU-B", "W2", "US", "B07", 28.0), ("SKU-B", "W1", None, "B08", 33.0),
    ("SKU-B", "W3", "MX", "B09", 40.0),
    ("SKU-C", "W2", "DE", "B10", 5.0), ("SKU-C", "W1", "IT", "B11", 6.5), ("SKU-C", "W2", "IT", "B12", 5.5),
    ("SKU-D", "W2", "CA", "B13", 18.0),
    ("SKU-E", "W1", "IT", "B14", 7.0),    # W2 has none: its NULL rolling cost sorts first (ASC = NULLS FIRST)
]

BLACKLISTED = [(9, "SKU-B", "W3"), (3, "SKU-C", "W1")]

SLA = [("W1", 24), ("W1", 48), ("W2", 72)]

# ---- real003: IoT telemetry (JSON payloads, UNPIVOT, LATERAL join, FULL OUTER JOIN) ----
TELEMETRY_DDL = "device_id STRING, event_timestamp TIMESTAMP, firmware_id STRING, raw_status_mask INT, payload_json STRING"
CALIBRATION_DDL = ("device_id STRING, metric_name STRING, effective_from TIMESTAMP, "
                   "scale_multiplier DOUBLE, calibration_offset DOUBLE")
INCIDENTS_DDL = "device_id STRING, incident_time TIMESTAMP, incident_severity STRING, incident_code STRING"
DEVICE_META_DDL = "device_id STRING, region_code STRING"


def _payload(temp, volts, vib):
    parts = [f'"temp": {"null" if temp is None else temp}', f'"voltage": {volts}', f'"vibration": {vib}',
             '"metrics_arr": [1.0, 2.0]']
    return "{" + ", ".join(parts) + "}"


TELEMETRY = [
    ("D1", "2026-03-01 10:05", "F1", 0, _payload(40.0, 3.3, 12.0)),
    ("D1", "2026-03-01 11:05", "F1", 8, _payload(None, 3.1, 15.0)),     # fault bit set, no temperature
    ("D1", "2026-03-01 12:05", "F1", 0, "not-json"),                     # unparseable payload
    ("D1", "2026-02-28 23:00", "F1", 0, _payload(39.0, 3.2, 11.0)),     # before March
    ("D2", "2026-03-01 10:20", "F1", 9, _payload(55.5, 4.8, 30.0)),
    ("D2", "2026-03-02 09:00", "F2", 2, _payload(51.0, 4.7, 28.0)),
    ("D3", "2026-03-01 10:45", "F2", 0, _payload(38.0, 3.0, 9.0)),      # no calibration -> dropped
    (None, "2026-03-01 10:00", "F1", 0, _payload(20.0, 1.0, 1.0)),      # no device
]

CALIBRATION = [
    ("D1", "TEMPERATURE", "2026-02-01 00:00", 1.0, 0.5),
    ("D1", "TEMPERATURE", "2026-03-01 11:00", 1.02, 0.0),
    ("D1", "VOLTAGE", "2026-01-01 00:00", 1.0, None),
    ("D1", "VIBRATION", "2026-01-01 00:00", None, 1.0),
    ("D2", "TEMPERATURE", "2026-02-15 00:00", 0.98, 1.2),
    ("D2", "VOLTAGE", "2026-02-15 00:00", 1.0, 0.0),
    ("D2", "VIBRATION", "2026-03-05 00:00", 1.1, 0.0),                   # not yet effective
]

INCIDENTS = [
    ("D1", "2026-03-01 10:05", "CRITICAL", "E-101"),                      # same time as an event
    ("D2", "2026-03-01 10:50", "WARNING", "W-007"),
    ("D3", "2026-03-01 10:45", "CRITICAL", "E-300"),
    ("D4", "2026-03-03 08:00", "INFO", "I-001"),
]

DEVICE_META = [("D1", "APAC"), ("D2", "EU"), ("D4", "US")]

# ---- real004: trades (regex, maps, ZIP_WITH / AGGREGATE lambdas, gaps and islands, CUBE) ----
TRADES_DDL = ("trade_id INT, account_id STRING, trade_timestamp TIMESTAMP, raw_symbol STRING, "
              "execution_details MAP<STRING, STRING>, fee_tier_rates ARRAY<DOUBLE>, "
              "applicable_multipliers ARRAY<DOUBLE>")
SETTLEMENTS_DDL = "account_id STRING, burst_island_id BIGINT"


def _fill(price, qty):
    return {"fill_price": price, "quantity": qty}


FEES, MULTS = [0.001, 0.0005], [1.0, 0.5]
TRADES = [
    (1, "A1", "2026-06-01 09:00", "BTC/USD", _fill("100.0", "2"), FEES, MULTS),
    (2, "A1", "2026-06-01 09:05", "BTC/USD", _fill("112.0", "1"), FEES, MULTS),
    (3, "A1", "2026-06-01 09:12", "ETH-USD", _fill("104.0", "3"), FEES, MULTS),
    (4, "A1", "2026-06-01 10:00", "ETH-USD", _fill("50.0", "4"), FEES, MULTS),     # new burst
    (5, "A1", "2026-06-01 10:03", "ETH-USD", _fill("51.0", "2"), FEES, MULTS),
    (6, "A2", "2026-06-02 14:00", "DOGE/USDT", _fill("0.2", "1000"), [0.002], [1.0]),
    (7, "A2", "2026-06-02 14:30", "BTC/USD", _fill("101.0", "1"), FEES, MULTS),    # new burst
    (8, "A2", "2026-06-02 14:33", "BTC/USD", _fill("99.5", "2"), FEES, MULTS),
    (9, "A3", "2026-06-03 08:00", "btcusd", _fill("100.0", "1"), FEES, MULTS),     # no regex match
    (10, "A3", "2026-06-03 08:05", "BTC/USD", _fill("abc", "1"), FEES, MULTS),     # bad price
    (11, "A1", "2026-05-31 23:59", "BTC/USD", _fill("90.0", "1"), FEES, MULTS),    # before June
    (12, "A2", "2026-06-02 15:00", "BTC/USD", None, FEES, MULTS),                  # no details
]

SETTLEMENTS = [("A1", 0), ("A1", 0), ("A1", 1), ("A2", 1), ("A3", 0), ("A3", 5)]


def make_dataframes(spark) -> dict:
    """Return {"df_<table>": DataFrame} and register each table as a temp view."""
    data = {
        "raw_events": ([(u, s, _ts(t), e, a, c) for u, s, t, e, a, c in RAW_EVENTS], RAW_EVENTS_DDL),
        "marketing_campaigns": ([(i, ty, ch, _ts(a), _ts(b)) for i, ty, ch, a, b in CAMPAIGNS], CAMPAIGNS_DDL),
        "fraud_blacklist": (BLACKLIST, BLACKLIST_DDL),
        "orders": ([(i, c, _ts(t), a, items, whs, flags) for i, c, t, a, items, whs, flags in ORDERS], ORDERS_DDL),
        "warehouse_inventory": (INVENTORY, INVENTORY_DDL),
        "blacklisted_fulfillments": (BLACKLISTED, BLACKLISTED_DDL),
        "warehouse_sla": (SLA, SLA_DDL),
        "sensor_telemetry": ([(d, _ts(t), f, m, p) for d, t, f, m, p in TELEMETRY], TELEMETRY_DDL),
        "calibration_registry": ([(d, n, _ts(t), a, b) for d, n, t, a, b in CALIBRATION], CALIBRATION_DDL),
        "operational_incidents": ([(d, _ts(t), sv, c) for d, t, sv, c in INCIDENTS], INCIDENTS_DDL),
        "device_metadata": (DEVICE_META, DEVICE_META_DDL),
        "trade_stream": ([(i, a, _ts(t), s, d, f, m) for i, a, t, s, d, f, m in TRADES], TRADES_DDL),
        "cleared_settlement_batches": (SETTLEMENTS, SETTLEMENTS_DDL),
    }
    frames = {}
    for name, (rows, ddl) in data.items():
        df = spark.createDataFrame(rows, ddl)
        df.createOrReplaceTempView(name)
        frames[f"df_{name}"] = df
    return frames
