WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, value
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        value,
        CONCAT(host, '/', level) AS host_level
    FROM flattened
    WHERE log_ts >= TIMESTAMP '2025-06-01 09:00:00'
      AND status_code >= 401
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN k = 'trace' THEN 1 ELSE 0 END AS is_trace
    FROM cleaned
    WHERE status_code >= 310
),
cleaned_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(is_trace, 2) AS is_trace_rounded,
        CAST(latency_ms AS DOUBLE) AS latency_ms_str,
        COALESCE(k, 'NONE') AS resolved_k
    FROM valid
    WHERE log_ts >= TO_DATE('2025-06-01')
),
valid_app_logs AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        host_level,
        is_trace,
        is_trace_rounded,
        latency_ms_str,
        resolved_k,
        CEIL(status_code) AS status_code_whole,
        COALESCE(k, 'N/A') AS resolved_k_2
    FROM cleaned_app_logs
    WHERE latency_ms_str <= 58
)
-- Grouped metrics
SELECT
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    MAX(log_ts) AS ended_at,
    SUM(CASE WHEN level = 'INFO' THEN status_code ELSE 0 END) AS info_status_code,
    COUNT(*) FILTER (WHERE k = 'trace') AS trace_rows,
    SUM(latency_ms) FILTER (WHERE k = 'trace') AS trace_total_latency_ms
FROM valid_app_logs
GROUP BY DATE_TRUNC('DAY', log_ts)
ORDER BY activity_day DESC NULLS LAST
