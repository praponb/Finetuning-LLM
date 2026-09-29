SELECT
    id,
    service,
    host,
    payload_json,
    context,
    FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries AS retry_count,
    GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier,
    FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier,
    GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id
FROM app_logs
