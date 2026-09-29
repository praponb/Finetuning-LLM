SELECT
    id,
    host,
    log_ts,
    latency_ms,
    attr_key,
    v
FROM app_logs
LATERAL VIEW EXPLODE(context) li AS attr_key, v;
