select
    id,
    service,
    log_ts,
    latency_ms,
    context,
    context['region'] as region,
    from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration as duration_sec,
    get_json_object(payload_json, '$.user.id') as user_id,
    shiftright(status_code, 2) as status_code_high,
    cast((status_code & 2) != 0 as int) as flag_bit2
from app_logs;
