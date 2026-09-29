WITH lines AS (
    SELECT
        id,
        service,
        log_ts,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, v
)
SELECT
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    attr_key,
    COUNT(DISTINCT service) AS distinct_services,
    ROUND(AVG(status_code), 2) AS avg_status_code,
    (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS duration_min,
    GROUPING(DATE_TRUNC('DAY', log_ts)) AS grouping_level
FROM lines
GROUP BY ROLLUP (DATE_TRUNC('DAY', log_ts), attr_key)
ORDER BY duration_min
