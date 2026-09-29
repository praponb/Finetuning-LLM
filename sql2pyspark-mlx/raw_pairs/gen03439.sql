select
    id,
    log_ts,
    level,
    status_code,
    payload_json,
    from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier as tier,
    coalesce(context['region'], 'unknown') as region_or_default,
    array_contains(map_keys(context), 'version') as has_version,
    from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries as retry_count
from app_logs
