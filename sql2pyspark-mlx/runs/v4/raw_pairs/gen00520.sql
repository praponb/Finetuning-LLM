SELECT
    id,
    service,
    host,
    log_ts,
    key,
    attr_value
FROM app_logs
LATERAL VIEW OUTER EXPLODE(context) e AS key, attr_value
