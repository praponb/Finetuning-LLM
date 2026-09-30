SELECT
    id,
    latency_ms,
    status_code,
    context,
    GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
    SHIFTRIGHT(status_code, 2) AS status_code_high,
    context['trace'] AS trace
FROM app_logs;
