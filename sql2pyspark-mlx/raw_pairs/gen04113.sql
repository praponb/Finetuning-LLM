SELECT
    id,
    host,
    log_ts,
    latency_ms,
    context['trace'] AS trace,
    SIZE(context) AS num_context,
    COALESCE(context['version'], 'none') AS version_or_default,
    SHIFTRIGHT(status_code, 2) AS status_code_high,
    GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text
FROM app_logs
