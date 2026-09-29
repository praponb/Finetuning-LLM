WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, value
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(latency_ms) OVER (PARTITION BY host ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS latency_ms_max_15m,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY TO_DATE(log_ts), host), 4) AS latency_ms_share
    FROM exploded
),
scored_app_logs AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY MONTH(log_ts)), 4) AS latency_ms_share_2,
        COUNT(latency_ms) OVER (PARTITION BY host, level) AS host_count_latency_ms
    FROM scored
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        level,
        attr_key,
        latency_ms_max_15m,
        host_count_latency_ms,
        CUME_DIST() OVER (PARTITION BY attr_key, host ORDER BY latency_ms_share_2 ASC NULLS FIRST) AS latency_ms_share_2_cume_dist,
        DENSE_RANK() OVER (PARTITION BY host, attr_key ORDER BY latency_ms_max_15m DESC) AS latency_ms_max_15m_dense_rank,
        DENSE_RANK() OVER (PARTITION BY attr_key ORDER BY latency_ms_share DESC NULLS FIRST) AS latency_ms_share_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY host ORDER BY host_count_latency_ms ASC NULLS FIRST) AS host_count_latency_ms_percent_rank
    FROM scored_app_logs
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        latency_ms_share_2_cume_dist,
        PERCENT_RANK() OVER (ORDER BY latency_ms_share_2_cume_dist DESC NULLS LAST) AS latency_ms_share_2_cume_dist_percent_rank
    FROM metrics
    WHERE latency_ms_share_2_cume_dist < 0.65
),
metrics_app_logs AS (
    -- Running and moving aggregates
    SELECT
        id,
        latency_ms_share_2_cume_dist,
        RANK() OVER (ORDER BY latency_ms_share_2_cume_dist DESC) AS latency_ms_share_2_cume_dist_rank
    FROM with_windows
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN latency_ms_share_2_cume_dist >= 0.63 THEN 'HIGH' ELSE 'LOW' END AS latency_ms_share_2_cume_dist_tier,
    COUNT(*) AS event_count,
    PERCENTILE_APPROX(latency_ms_share_2_cume_dist_rank, 0.95) AS latency_ms_share_2_cume_dist_rank_p95
FROM metrics_app_logs
GROUP BY CASE WHEN latency_ms_share_2_cume_dist >= 0.63 THEN 'HIGH' ELSE 'LOW' END;
