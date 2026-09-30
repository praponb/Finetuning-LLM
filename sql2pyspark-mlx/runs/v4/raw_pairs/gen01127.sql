SELECT
    id,
    service,
    host,
    log_ts,
    level,
    status_code,
    k,
    value
FROM app_logs
LATERAL VIEW EXPLODE(context) x AS k, value
