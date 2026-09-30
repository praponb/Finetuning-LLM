WITH unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        service,
        level,
        latency_ms,
        status_code,
        context,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 2) != 0 AS INT) AS has_fault,
        COALESCE(context['version'], 'none') AS version_or_default,
        COALESCE(context['trace'], 'unknown') AS trace_or_default
    FROM app_logs
)
-- Aggregate with conditional metrics
SELECT
    service,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    COUNT(CASE WHEN service = 'search' THEN 1 END) AS search_count,
    COUNT(*) FILTER (WHERE service = 'auth') AS auth_rows
FROM unpacked
GROUP BY service
ORDER BY search_count DESC NULLS LAST;
