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
    LATERAL VIEW OUTER EXPLODE(context) e AS k, v
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        log_ts,
        level,
        v,
        DENSE_RANK() OVER (ORDER BY latency_ms DESC) AS latency_ms_dense_rank,
        AVG(latency_ms) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS latency_ms_avg_15m,
        COUNT(latency_ms) OVER (PARTITION BY k) AS k_count_latency_ms
    FROM exploded
    WHERE k != 'version'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        log_ts,
        level,
        v,
        latency_ms_dense_rank,
        MAX(k_count_latency_ms) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS k_count_latency_ms_max_2h,
        CUME_DIST() OVER (PARTITION BY level ORDER BY latency_ms_dense_rank DESC NULLS FIRST) AS latency_ms_dense_rank_cume_dist,
        ROUND(latency_ms_dense_rank / SUM(latency_ms_dense_rank) OVER (PARTITION BY level), 4) AS latency_ms_dense_rank_share,
        ROUND(latency_ms_avg_15m / SUM(latency_ms_avg_15m) OVER (PARTITION BY level), 4) AS latency_ms_avg_15m_share
    FROM metrics
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        level,
        latency_ms_avg_15m_share,
        ROUND(latency_ms_dense_rank / SUM(latency_ms_dense_rank) OVER (PARTITION BY level), 4) AS latency_ms_dense_rank_share_2,
        CUME_DIST() OVER (PARTITION BY level ORDER BY latency_ms_dense_rank) AS latency_ms_dense_rank_cume_dist_2,
        PERCENT_RANK() OVER (PARTITION BY level ORDER BY k_count_latency_ms_max_2h NULLS FIRST) AS k_count_latency_ms_max_2h_percent_rank,
        DENSE_RANK() OVER (ORDER BY latency_ms_dense_rank DESC NULLS LAST) AS latency_ms_dense_rank_dense_rank,
        SUM(latency_ms_dense_rank) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS latency_ms_dense_rank_sum_30m
    FROM scored
)
-- Window metrics over several different partitions
SELECT
    latency_ms_avg_15m_share,
    latency_ms_dense_rank_cume_dist_2,
    CUME_DIST() OVER (PARTITION BY level ORDER BY k_count_latency_ms_max_2h_percent_rank) AS k_count_latency_ms_max_2h_percent_rank_cume_dist,
    COUNT(latency_ms_dense_rank_share_2) OVER (PARTITION BY level) AS level_count_latency_ms_dense_rank_share_2,
    PERCENT_RANK() OVER (ORDER BY latency_ms_dense_rank_dense_rank NULLS LAST) AS latency_ms_dense_rank_dense_rank_percent_rank,
    RANK() OVER w_ordered AS latency_ms_dense_rank_sum_30m_rank
FROM with_windows
WINDOW w_ordered AS (PARTITION BY level ORDER BY latency_ms_dense_rank_sum_30m DESC NULLS FIRST)
