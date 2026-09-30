SELECT
    id,
    service,
    log_ts,
    level,
    latency_ms,
    status_code,
    payload_json,
    context['trace'] AS trace,
    SHIFTRIGHT(status_code, 2) AS status_code_high
FROM app_logs
