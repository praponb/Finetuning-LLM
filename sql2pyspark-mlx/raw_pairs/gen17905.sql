SELECT
    id,
    service,
    host,
    level,
    status_code,
    SHIFTRIGHT(status_code, 2) AS status_code_high,
    GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier,
    CAST((status_code & 2) != 0 AS INT) AS has_fault,
    COALESCE(context['trace'], 'unknown') AS trace_or_default
FROM app_logs
