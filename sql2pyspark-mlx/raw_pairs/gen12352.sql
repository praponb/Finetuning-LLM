WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        level,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, value
)
-- Multi-level aggregation
SELECT
    level,
    COUNT(*) FILTER (WHERE level = 'WARN') AS warn_rows,
    COUNT(*) FILTER (WHERE level = 'ERROR') AS error_rows,
    SORT_ARRAY(COLLECT_SET(service)) AS service_list,
    COUNT(CASE WHEN level = 'DEBUG' THEN 1 END) AS debug_count,
    SORT_ARRAY(COLLECT_SET(service)) AS service_list_2,
    COUNT(CASE WHEN service = 'billing' THEN 1 END) AS billing_count
FROM exploded
GROUP BY ROLLUP (level)
HAVING COUNT(*) FILTER (WHERE level = 'WARN') > 1
