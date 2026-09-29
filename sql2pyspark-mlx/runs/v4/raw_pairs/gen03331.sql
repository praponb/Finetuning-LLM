select
    id,
    host,
    log_ts,
    payload_json,
    context,
    get_json_object(payload_json, '$.user.id') as user_id,
    context['trace'] as trace,
    context['version'] as version,
    coalesce(context['trace'], 'none') as trace_or_default,
    shiftright(status_code, 2) as status_code_high
from app_logs;
