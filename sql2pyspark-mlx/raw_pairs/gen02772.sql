select
    id,
    service,
    level,
    latency_ms,
    status_code,
    payload_json,
    context,
    coalesce(context['trace'], 'unknown') as trace_or_default,
    shiftright(status_code, 2) as status_code_high
from app_logs
