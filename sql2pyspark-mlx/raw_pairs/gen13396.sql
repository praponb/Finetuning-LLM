select
    id,
    service,
    host,
    log_ts,
    latency_ms,
    payload_json,
    context,
    from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries as retry_count,
    cast((status_code & 2) != 0 as int) as has_fault
from app_logs;
