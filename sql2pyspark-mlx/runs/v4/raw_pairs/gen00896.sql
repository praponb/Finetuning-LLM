SELECT
    id,
    host,
    latency_ms,
    payload_json,
    context,
    GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier,
    SIZE(context) AS num_context,
    ARRAY_CONTAINS(MAP_KEYS(context), 'region') AS has_region
FROM app_logs
WHERE host = 'h4'
