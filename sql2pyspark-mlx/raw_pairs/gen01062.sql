SELECT
    id,
    service,
    status_code,
    payload_json,
    context,
    GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
    FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier,
    SHIFTRIGHT(status_code, 2) AS status_code_high,
    CAST((status_code & 8) != 0 AS INT) AS has_fault,
    COALESCE(context['version'], 'none') AS version_or_default
FROM app_logs
WHERE service != 'checkout';
