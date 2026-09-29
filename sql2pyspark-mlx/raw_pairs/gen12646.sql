SELECT
    id,
    service,
    k,
    attr_value
FROM app_logs
LATERAL VIEW OUTER EXPLODE(context) li AS k, attr_value
