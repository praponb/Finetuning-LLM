select
    id,
    service,
    host,
    log_ts,
    level,
    latency_ms,
    status_code,
    payload_json,
    from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries as retry_count,
    size(context) as num_context,
    get_json_object(payload_json, '$.duration') as duration_text,
    shiftright(status_code, 2) as status_code_high,
    coalesce(context['version'], 'none') as version_or_default
from app_logs;
