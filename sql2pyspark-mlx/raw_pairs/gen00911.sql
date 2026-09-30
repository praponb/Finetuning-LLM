WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS k, attr_value
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(status_code) OVER w_grp AS status_code_max_30m,
        MAX(status_code) OVER w_part AS status_code_max_30m_2,
        CUME_DIST() OVER (PARTITION BY k, host ORDER BY latency_ms NULLS LAST) AS latency_ms_cume_dist,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY DATE_TRUNC('YEAR', log_ts), k), 4) AS latency_ms_share,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY DATE_TRUNC('MONTH', log_ts), k), 4) AS latency_ms_share_2
    FROM exploded
    WINDOW w_grp AS (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY host ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        latency_ms,
        status_code,
        k,
        attr_value,
        status_code_max_30m,
        status_code_max_30m_2,
        CORR(latency_ms, status_code) OVER (PARTITION BY k) AS corr_latency_ms_status_code,
        DENSE_RANK() OVER (PARTITION BY host ORDER BY status_code) AS status_code_dense_rank,
        PERCENT_RANK() OVER w_running AS status_code_max_30m_2_percent_rank,
        AVG(status_code_max_30m_2) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_code_max_30m_2_avg_30m
    FROM metrics
    WINDOW w_running AS (PARTITION BY host, k ORDER BY status_code_max_30m_2 NULLS FIRST)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(status_code_dense_rank) OVER (PARTITION BY k) AS k_sum_status_code_dense_rank,
        ROUND(status_code_max_30m_2_avg_30m / SUM(status_code_max_30m_2_avg_30m) OVER (PARTITION BY k), 4) AS status_code_max_30m_2_avg_30m_share,
        RANK() OVER (PARTITION BY k ORDER BY status_code_max_30m_2_avg_30m DESC NULLS LAST) AS status_code_max_30m_2_avg_30m_rank
    FROM scored
),
metrics_app_logs AS (
    -- Running and moving aggregates
    SELECT
        latency_ms,
        status_code,
        k,
        attr_value,
        status_code_max_30m,
        status_code_dense_rank,
        status_code_max_30m_2_avg_30m,
        status_code_max_30m_2_avg_30m_share,
        status_code_max_30m_2_avg_30m_rank,
        PERCENT_RANK() OVER (ORDER BY status_code_max_30m_2_avg_30m DESC NULLS LAST) AS status_code_max_30m_2_avg_30m_percent_rank,
        ROUND(status_code_dense_rank / SUM(status_code_dense_rank) OVER (PARTITION BY k), 4) AS status_code_dense_rank_share,
        DENSE_RANK() OVER (PARTITION BY k ORDER BY corr_latency_ms_status_code DESC NULLS LAST) AS corr_latency_ms_status_code_dense_rank
    FROM with_windows
    WHERE status_code_max_30m_2_avg_30m_share >= 0.39
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        latency_ms,
        k,
        attr_value,
        status_code_max_30m,
        status_code_dense_rank,
        status_code_max_30m_2_avg_30m_rank,
        status_code_dense_rank_share,
        corr_latency_ms_status_code_dense_rank,
        CEIL(status_code_dense_rank_share) AS status_code_dense_rank_share_whole
    FROM metrics_app_logs
    WHERE k IS NOT NULL
)
SELECT
    latency_ms,
    k,
    status_code_max_30m_2_avg_30m_rank,
    status_code_dense_rank_share,
    status_code_dense_rank_share_whole
FROM cleaned
ORDER BY status_code_max_30m_2_avg_30m_rank NULLS LAST;
