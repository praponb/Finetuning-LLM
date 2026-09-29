WITH flattened AS (
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, v
)
SELECT
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    CASE WHEN status_code >= 432 THEN 'HIGH' ELSE 'LOW' END AS status_code_tier,
    COUNT(*) FILTER (WHERE key = 'version') AS version_rows,
    COUNT(CASE WHEN level = 'DEBUG' THEN 1 END) AS debug_count
FROM flattened
GROUP BY GROUPING SETS ((DATE_TRUNC('HOUR', log_ts), CASE WHEN status_code >= 432 THEN 'HIGH' ELSE 'LOW' END), (DATE_TRUNC('HOUR', log_ts)))
HAVING version_rows > 1
