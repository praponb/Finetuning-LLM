SELECT
    id,
    service,
    log_ts,
    status_code,
    payload_json,
    COALESCE(context['trace'], 'direct') AS trace_or_default,
    GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id,
    GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier,
    ARRAY_CONTAINS(MAP_KEYS(context), 'region') AS has_region
FROM app_logs;
