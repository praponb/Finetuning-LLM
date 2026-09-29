SELECT
    id,
    service,
    host,
    log_ts,
    level,
    k,
    v
FROM app_logs
LATERAL VIEW OUTER EXPLODE(context) li AS k, v;
