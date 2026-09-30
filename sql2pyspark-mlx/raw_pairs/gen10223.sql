SELECT
    id,
    service,
    level,
    latency_ms,
    payload_json,
    GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id,
    context['region'] AS region,
    GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier,
    FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec
FROM app_logs;
