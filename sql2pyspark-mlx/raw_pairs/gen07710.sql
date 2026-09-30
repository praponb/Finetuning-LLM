WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        level,
        latency_ms,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, v
)
-- Grouped metrics
SELECT
    level,
    PERCENTILE_APPROX(latency_ms, 0.5) AS latency_ms_p50,
    COUNT(CASE WHEN level = 'INFO' THEN 1 END) AS info_count,
    SORT_ARRAY(COLLECT_SET(key)) AS key_list,
    SUM(CASE WHEN level = 'INFO' THEN latency_ms ELSE 0 END) AS info_latency_ms,
    COUNT(CASE WHEN service = 'search' THEN 1 END) AS search_count
FROM exploded
GROUP BY level
ORDER BY info_count DESC;
