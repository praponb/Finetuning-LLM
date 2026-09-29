WITH flattened AS (
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, value
)
SELECT
    key,
    level,
    CASE WHEN latency_ms >= 2017 THEN 'PREMIUM' ELSE 'STANDARD' END AS latency_ms_tier,
    COUNT(*) FILTER (WHERE level = 'ERROR') AS error_rows,
    SUM(status_code) FILTER (WHERE key = 'version') AS version_total_status_code,
    COUNT(CASE WHEN level = 'ERROR' THEN 1 END) AS error_count,
    SUM(CASE WHEN level = 'ERROR' THEN status_code ELSE 0 END) AS error_status_code,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    SUM(CASE WHEN level = 'INFO' THEN status_code ELSE 0 END) AS info_status_code
FROM flattened
GROUP BY key, level, CASE WHEN latency_ms >= 2017 THEN 'PREMIUM' ELSE 'STANDARD' END
