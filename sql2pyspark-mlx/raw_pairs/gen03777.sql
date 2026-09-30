select
    id,
    service,
    host,
    from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries as retry_count,
    shiftright(status_code, 2) as status_code_high,
    context['version'] as version,
    array_contains(map_keys(context), 'version') as has_version
from app_logs
where host in ('h1', 'h4')
