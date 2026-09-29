WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        level,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS attr_key, value
)
-- Multi-level aggregation
SELECT
    level,
    SUM(status_code) FILTER (WHERE attr_key = 'trace') AS trace_total_status_code,
    SORT_ARRAY(COLLECT_SET(attr_key)) AS attr_key_list
FROM flattened
GROUP BY level
ORDER BY level ASC
