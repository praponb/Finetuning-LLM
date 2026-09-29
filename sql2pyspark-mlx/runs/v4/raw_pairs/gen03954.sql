select
    id,
    host,
    log_ts,
    latency_ms,
    status_code,
    payload_json,
    from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries as retry_count,
    context['region'] as region,
    cast((status_code & 8) != 0 as int) as has_fault
from app_logs
