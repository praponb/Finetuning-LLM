SELECT
    id,
    host,
    level,
    latency_ms,
    ARRAY_CONTAINS(MAP_KEYS(context), 'region') AS has_region,
    FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier,
    GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
    SHIFTRIGHT(status_code, 2) AS status_code_high,
    context['trace'] AS trace
FROM app_logs
WHERE latency_ms <= 1227;
