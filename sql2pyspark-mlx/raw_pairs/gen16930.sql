WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, value
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(status_code AS DOUBLE) AS status_code_str
    FROM flattened
    WHERE log_ts > TIMESTAMP '2025-06-01 14:00:00'
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        level,
        status_code,
        k,
        value,
        SUBSTRING(level, 1, 3) AS level_prefix,
        CONCAT_WS('-', k, level) AS k_level_key
    FROM filtered
    WHERE log_ts > DATE '2025-06-01'
      AND k IS DISTINCT FROM 'region'
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        level,
        status_code,
        k,
        level_prefix,
        k_level_key,
        INITCAP(TRIM(level)) AS level_title
    FROM cleaned
    WHERE status_code >= 398
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(status_code) OVER w_ordered AS running_min_status_code,
        AVG(status_code) OVER w_ordered AS running_avg_status_code,
        SUM(status_code) OVER w_ordered AS running_sum_status_code,
        SUM(status_code) OVER (ORDER BY status_code, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_status_code
    FROM valid
    WINDOW w_ordered AS (PARTITION BY k ORDER BY status_code, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    level,
    COUNT_IF(k = 'region') AS region_events,
    SUM(CASE WHEN level = 'INFO' THEN cumulative_status_code ELSE 0 END) AS info_cumulative_status_code,
    SIZE(COLLECT_SET(level)) AS num_levels
FROM scored
GROUP BY level
HAVING region_events < 1000
ORDER BY num_levels DESC NULLS LAST
