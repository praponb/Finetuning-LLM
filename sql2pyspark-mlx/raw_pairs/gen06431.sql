WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, v
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY k, host), 4) AS status_code_share,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY MONTH(log_ts), k), 4) AS status_code_share_2
    FROM lines
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), log_ts) AS days_since_log_ts,
        COALESCE(k, 'UNKNOWN') AS k_clean
    FROM with_windows
    WHERE log_ts > TO_TIMESTAMP('2025-06-01 14:00:00')
      AND status_code_share > 0.23
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        log_ts,
        status_code_share,
        status_code_share_2,
        days_since_log_ts,
        k_clean,
        ROUND(status_code_share, 2) AS status_code_share_rounded
    FROM base
    WHERE log_ts >= TIMESTAMP '2025-06-01 06:00:00'
      AND k IS NOT NULL
)
-- Grouped metrics
SELECT
    CASE WHEN status_code_share >= 0.71 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_share_tier,
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    COUNT(*) AS num_rows,
    APPROX_PERCENTILE(status_code_share, 0.9) AS status_code_share_p90,
    MIN(log_ts) AS first_log_ts,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2,
    MIN(log_ts) AS started_at
FROM valid
GROUP BY ROLLUP (CASE WHEN status_code_share >= 0.71 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('DAY', log_ts))
ORDER BY distinct_ids DESC NULLS FIRST
