WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        level,
        latency_ms,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, v
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(latency_ms) OVER w_part AS level_max_latency_ms,
        COUNT(latency_ms) OVER w_part AS level_count_latency_ms,
        PERCENT_RANK() OVER (PARTITION BY service, level ORDER BY latency_ms DESC) AS latency_ms_percent_rank,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY level, service), 4) AS latency_ms_share
    FROM lines
    WINDOW w_part AS (PARTITION BY level)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        level,
        key,
        v,
        level_max_latency_ms,
        level_count_latency_ms,
        latency_ms_percent_rank,
        latency_ms_share,
        FLOOR(level_max_latency_ms) AS level_max_latency_ms_whole,
        COALESCE(key, 'OTHER') AS key_clean
    FROM metrics
    WHERE key IS NOT NULL
      AND level_count_latency_ms BETWEEN 8 AND 15
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        level,
        key,
        v,
        level_max_latency_ms,
        level_count_latency_ms,
        latency_ms_percent_rank,
        latency_ms_share,
        level_max_latency_ms_whole,
        key_clean,
        RANK() OVER (ORDER BY level_count_latency_ms DESC) AS level_count_latency_ms_rank,
        CUME_DIST() OVER (PARTITION BY level, key ORDER BY latency_ms_share DESC NULLS FIRST) AS latency_ms_share_cume_dist,
        RANK() OVER (PARTITION BY key ORDER BY latency_ms_percent_rank NULLS FIRST) AS latency_ms_percent_rank_rank
    FROM cleaned
),
with_windows_app_logs AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(level_count_latency_ms_rank) OVER (PARTITION BY level) AS level_min_level_count_latency_ms_rank,
        RANK() OVER (PARTITION BY key ORDER BY level_max_latency_ms DESC NULLS FIRST) AS level_max_latency_ms_rank
    FROM with_windows
),
metrics_app_logs AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        level,
        key,
        v,
        level_max_latency_ms,
        level_count_latency_ms,
        latency_ms_percent_rank,
        latency_ms_share,
        level_max_latency_ms_whole,
        key_clean,
        level_count_latency_ms_rank,
        latency_ms_share_cume_dist,
        latency_ms_percent_rank_rank,
        level_min_level_count_latency_ms_rank,
        level_max_latency_ms_rank,
        PERCENT_RANK() OVER (PARTITION BY level ORDER BY level_max_latency_ms_whole DESC NULLS LAST) AS level_max_latency_ms_whole_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY key, level ORDER BY latency_ms_percent_rank_rank NULLS FIRST) AS latency_ms_percent_rank_rank_percent_rank
    FROM with_windows_app_logs
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN level_max_latency_ms >= 1918 THEN 'HIGH' ELSE 'LOW' END AS level_max_latency_ms_tier,
    key,
    APPROX_PERCENTILE(level_min_level_count_latency_ms_rank, ARRAY(0.25, 0.5, 0.75)) AS level_min_level_count_latency_ms_rank_quartiles,
    SUM(level_max_latency_ms) AS total_level_max_latency_ms
FROM metrics_app_logs
GROUP BY CASE WHEN level_max_latency_ms >= 1918 THEN 'HIGH' ELSE 'LOW' END, key
ORDER BY total_level_max_latency_ms
