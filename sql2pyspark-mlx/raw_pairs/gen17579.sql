WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, value
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM exploded
    QUALIFY ROW_NUMBER() OVER (PARTITION BY level ORDER BY log_ts DESC, id, k) = 1
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        service,
        log_ts,
        level,
        k,
        value,
        CONCAT_WS('-', k, level) k_level_key
    FROM latest
    WHERE log_ts >= DATE '2025-06-01'
      AND k IS NOT NULL
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN log_ts - LAG(log_ts) OVER (PARTITION BY level ORDER BY log_ts, id, k) > INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS new_group_flag
    FROM base
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY level ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(island_id) OVER (PARTITION BY k ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) island_id_max_15m,
        SUM(island_id) OVER (ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_island_id,
        ROW_NUMBER() OVER (PARTITION BY k ORDER BY island_id, id, k) seq_no
    FROM islands
    WHERE level IN ('WARN', 'INFO', 'DEBUG')
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN new_group_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS new_group_flag_tier,
    MAX(log_ts) AS last_log_ts,
    COUNT(DISTINCT level) AS distinct_levels,
    APPROX_PERCENTILE(cumulative_island_id, 0.9) AS cumulative_island_id_p90
FROM with_windows
GROUP BY new_group_flag_tier
HAVING cumulative_island_id_p90 < 1000000;
