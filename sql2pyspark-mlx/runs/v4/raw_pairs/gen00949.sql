SELECT
    id,
    service,
    latency_ms,
    status_code,
    payload_json,
    context,
    FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries AS retry_count,
    GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier
FROM app_logs
