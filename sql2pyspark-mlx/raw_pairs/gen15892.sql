SELECT
    id,
    log_ts,
    level,
    payload_json,
    FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier,
    GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
    SIZE(context) AS num_context,
    FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries AS retry_count
FROM app_logs
WHERE TO_DATE(log_ts) >= '2024-10-01';
