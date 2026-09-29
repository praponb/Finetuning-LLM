select
    id,
    host,
    log_ts,
    level,
    context,
    get_json_object(payload_json, '$.user.tier') as user_tier,
    coalesce(context['version'], 'unknown') as version_or_default,
    shiftright(status_code, 2) as status_code_high
from app_logs;
