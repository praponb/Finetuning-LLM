select
    id,
    service,
    host,
    log_ts,
    get_json_object(payload_json, '$.duration') as duration_text,
    array_contains(map_keys(context), 'version') as has_version,
    coalesce(context['version'], 'none') as version_or_default
from app_logs
where log_ts < '2025-02-18'
