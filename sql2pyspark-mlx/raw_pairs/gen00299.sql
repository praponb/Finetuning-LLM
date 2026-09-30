SELECT
    id,
    host,
    status_code,
    attr_key,
    v
FROM app_logs
LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, v;
