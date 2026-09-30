WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS k, v
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM exploded
    INTERSECT
    SELECT id
    FROM app_logs
    WHERE latency_ms BETWEEN 1081 AND 1758
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM exploded s
    JOIN matched_keys k ON s.id = k.id
),
base AS (
    -- Keep recent, valid rows
    SELECT *, DAY(log_ts) AS log_ts_day
    FROM verified
    WHERE log_ts > DATE '2025-06-01'
      AND k IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        latency_ms,
        k,
        log_ts_day,
        STDDEV_SAMP(latency_ms) OVER (PARTITION BY k) AS k_stddev_samp_latency_ms,
        SUM(latency_ms) OVER w_part AS k_sum_latency_ms,
        MAX(log_ts_day) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS log_ts_day_max_30m
    FROM base
    WINDOW w_part AS (PARTITION BY k, level)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        latency_ms,
        k,
        log_ts_day,
        k_stddev_samp_latency_ms,
        k_sum_latency_ms,
        log_ts_day_max_30m,
        GREATEST(log_ts_day, latency_ms) AS greatest_log_ts_day_latency_ms,
        COALESCE(k, 'OTHER') AS resolved_k
    FROM scored
    WHERE latency_ms != 1878
),
valid_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        latency_ms,
        k,
        log_ts_day,
        k_sum_latency_ms,
        log_ts_day_max_30m,
        greatest_log_ts_day_latency_ms,
        resolved_k,
        ROUND(greatest_log_ts_day_latency_ms * 1.1, 2) AS adjusted_greatest_log_ts_day_latency_ms,
        COALESCE(k, 'DIRECT') AS resolved_k_2
    FROM valid
    WHERE greatest_log_ts_day_latency_ms BETWEEN 32 AND 52
)
-- Aggregate with conditional metrics
SELECT
    k,
    COUNT(CASE WHEN k = 'region' THEN 1 END) AS region_count,
    MAX(log_ts_day) FILTER (WHERE k = 'region') AS region_max_log_ts_day,
    COUNT(*) AS cnt
FROM valid_app_logs
GROUP BY ROLLUP (k)
