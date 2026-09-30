WITH unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        service,
        host,
        level,
        context,
        ARRAY_CONTAINS(MAP_KEYS(context), 'version') AS has_version,
        SIZE(context) AS num_context
    FROM app_logs
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN num_context >= 88 THEN 'LARGE' ELSE 'SMALL' END AS num_context_tier,
    COUNT_IF(level = 'ERROR') AS error_events,
    COUNT(CASE WHEN service = 'billing' THEN 1 END) AS billing_count,
    COUNT(*) FILTER (WHERE service = 'checkout') AS checkout_rows,
    SORT_ARRAY(COLLECT_SET(host)) AS host_list
FROM unpacked
GROUP BY CASE WHEN num_context >= 88 THEN 'LARGE' ELSE 'SMALL' END
