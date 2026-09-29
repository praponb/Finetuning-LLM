WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, value
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN host IN ('h1', 'h3') THEN 'Core' ELSE 'Other' END AS host_group,
        COALESCE(k, 'NONE') AS k_clean
    FROM flattened
    WHERE k IS NOT NULL
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LOWER(host) AS host_lower,
        COALESCE(k, 'NONE') AS k_filled
    FROM filtered
    WHERE log_ts >= TO_TIMESTAMP('2025-06-01 13:00:00')
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT *, COUNT(status_code) OVER (PARTITION BY k ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_code_count_30m
    FROM cleaned
)
-- Grouped metrics
SELECT
    CASE WHEN status_code >= 432 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_tier,
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    ROUND(SUM(status_code) / NULLIF(COUNT(CASE WHEN level = 'INFO' THEN 1 END), 0), 2) AS status_code_per_info,
    MAX(log_ts) AS ended_at,
    COUNT(DISTINCT id) AS distinct_ids,
    MAX(log_ts) AS last_log_ts,
    ROUND(SUM(status_code_count_30m) / NULLIF(COUNT(CASE WHEN host = 'h2' THEN 1 END), 0), 2) AS status_code_count_30m_per_h2,
    GROUPING_ID() AS grouping_level
FROM with_windows
GROUP BY ROLLUP (CASE WHEN status_code >= 432 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('HOUR', log_ts))
