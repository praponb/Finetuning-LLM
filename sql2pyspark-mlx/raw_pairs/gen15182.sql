SELECT
    id,
    host,
    log_ts,
    level,
    payload_json,
    FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec,
    GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier
FROM app_logs;
