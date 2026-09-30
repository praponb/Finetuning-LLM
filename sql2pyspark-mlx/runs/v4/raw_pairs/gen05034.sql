WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, v
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY latency_ms DESC) AS latency_ms_dense_rank,
        SUM(latency_ms) OVER (PARTITION BY host ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS latency_ms_sum_30m,
        COUNT(latency_ms) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS latency_ms_count_2h,
        SUM(latency_ms) OVER (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS latency_ms_sum_1h
    FROM exploded
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(latency_ms_count_2h / SUM(latency_ms_count_2h) OVER (PARTITION BY service), 4) AS latency_ms_count_2h_share,
        CUME_DIST() OVER (PARTITION BY level ORDER BY latency_ms_sum_30m NULLS FIRST) AS latency_ms_sum_30m_cume_dist,
        AVG(latency_ms) OVER (PARTITION BY host, level ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS latency_ms_avg_1h
    FROM scored
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(latency_ms_avg_1h) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS latency_ms_avg_1h_count_30m,
        SUM(latency_ms_sum_30m_cume_dist) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS latency_ms_sum_30m_cume_dist_sum_30m
    FROM metrics
)
SELECT
    service,
    host,
    v,
    latency_ms_dense_rank,
    latency_ms_count_2h,
    latency_ms_sum_1h,
    latency_ms_count_2h_share,
    latency_ms_sum_30m_cume_dist,
    latency_ms_avg_1h
FROM with_windows
ORDER BY latency_ms_dense_rank;
