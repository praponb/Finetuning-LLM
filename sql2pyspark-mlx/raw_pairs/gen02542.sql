WITH lines AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS attr_key, v
)
SELECT
    service,
    COUNT_IF(host = 'h4') AS h4_events,
    COUNT_IF(service = 'billing') AS billing_events
FROM lines
GROUP BY CUBE (service)
HAVING billing_events > 1
ORDER BY service DESC NULLS FIRST
