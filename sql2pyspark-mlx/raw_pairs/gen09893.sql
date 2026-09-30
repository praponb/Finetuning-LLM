WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, v
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        v,
        LENGTH(k) AS k_length,
        ROUND(latency_ms, 0) AS latency_ms_rounded,
        COALESCE(k, 'OTHER') AS k_clean
    FROM lines
    WHERE log_ts >= TO_DATE('2025-06-01')
      AND k IS NOT NULL
),
base_app_logs AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        log_ts,
        level,
        status_code,
        k,
        k_length,
        k_clean,
        COALESCE(k, 'unassigned') AS k_filled,
        COALESCE(k, 'UNKNOWN') AS k_clean_2
    FROM base
    WHERE k IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        level,
        k,
        k_length,
        k_filled,
        STDDEV_SAMP(k_length) OVER w_recent AS level_stddev_samp_k_length
    FROM base_app_logs
    WHERE HOUR(log_ts) BETWEEN 8 AND 14
    WINDOW w_recent AS (PARTITION BY level)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        k_length,
        k_filled,
        PERCENT_RANK() OVER (ORDER BY k_length) AS k_length_percent_rank,
        CUME_DIST() OVER (PARTITION BY k ORDER BY k_length DESC NULLS LAST) AS k_length_cume_dist,
        RANK() OVER (PARTITION BY level, k ORDER BY k_length NULLS LAST) AS k_length_rank,
        MAX(k_length) OVER (PARTITION BY level) AS level_max_k_length,
        DENSE_RANK() OVER (ORDER BY level_stddev_samp_k_length) AS level_stddev_samp_k_length_dense_rank
    FROM metrics
)
-- Multi-level aggregation
SELECT
    CASE WHEN k_length_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END AS k_length_rank_tier,
    APPROX_PERCENTILE(level_max_k_length, ARRAY(0.25, 0.5, 0.75)) AS level_max_k_length_quartiles,
    PERCENTILE_APPROX(k_length_cume_dist, 0.95) AS k_length_cume_dist_p95,
    AVG(k_length_rank) AS avg_k_length_rank
FROM with_windows
GROUP BY CASE WHEN k_length_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END
HAVING AVG(k_length_rank) > 0
ORDER BY k_length_cume_dist_p95 NULLS FIRST;
