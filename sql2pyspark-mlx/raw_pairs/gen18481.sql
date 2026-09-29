select
    id,
    status_code,
    get_json_object(payload_json, '$.duration') as duration_text,
    coalesce(context['version'], 'direct') as version_or_default,
    coalesce(context['region'], 'none') as region_or_default,
    context['trace'] as trace
from app_logs
where status_code <= 339;
