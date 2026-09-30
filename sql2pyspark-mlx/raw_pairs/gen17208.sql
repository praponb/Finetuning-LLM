SELECT
    id,
    host,
    log_ts,
    key,
    attr_value
FROM app_logs
LATERAL VIEW OUTER EXPLODE(context) li AS key, attr_value;
