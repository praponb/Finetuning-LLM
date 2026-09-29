WITH exploded AS (
    SELECT
        id,
        service,
        host,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS k, attr_value
)
SELECT
    service,
    latency_ms,
    status_code,
    CASE WHEN service RLIKE '^(billing|auth)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS service_zone
FROM exploded
ORDER BY service_zone DESC;
