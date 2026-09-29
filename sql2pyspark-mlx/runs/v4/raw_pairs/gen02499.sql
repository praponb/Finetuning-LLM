SELECT
    id,
    service,
    log_ts,
    latency_ms,
    attr_key,
    value
FROM app_logs
LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, value;
