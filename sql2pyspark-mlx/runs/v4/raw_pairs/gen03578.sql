SELECT
    id,
    host,
    level,
    status_code,
    payload_json,
    context,
    GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
    context['version'] AS version,
    GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id,
    FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec
FROM app_logs;
