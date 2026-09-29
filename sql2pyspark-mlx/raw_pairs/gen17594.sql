SELECT
    id,
    log_ts,
    level,
    latency_ms,
    status_code,
    k,
    attr_value
FROM app_logs
LATERAL VIEW OUTER EXPLODE(context) e AS k, attr_value
