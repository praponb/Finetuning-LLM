select
    id,
    service,
    level,
    latency_ms,
    status_code,
    payload_json,
    context,
    get_json_object(payload_json, '$.user.tier') as user_tier,
    size(context) as num_context
from app_logs
where latency_ms = 2229
