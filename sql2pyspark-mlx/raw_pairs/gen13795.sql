SELECT
    id,
    log_ts,
    level,
    status_code,
    k,
    v
FROM app_logs
LATERAL VIEW OUTER EXPLODE(context) e AS k, v;
