WITH flattened AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, v
)
SELECT
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    COUNT(CASE WHEN level = 'ERROR' THEN 1 END) AS error_count,
    COUNT(CASE WHEN level = 'WARN' THEN 1 END) AS warn_count,
    SIZE(COLLECT_SET(service)) AS num_services,
    COUNT(*) FILTER (WHERE service = 'billing') AS billing_rows
FROM flattened
GROUP BY 1
HAVING num_services > 1
ORDER BY num_services DESC
