WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, value
),
scored AS (
    -- Window metrics over several different partitions
    SELECT *, AVG(status_code) OVER (PARTITION BY k ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_code_avg_2h
    FROM flattened
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, TRIM(host) AS clean_host
    FROM scored
    WHERE log_ts >= TO_DATE('2025-06-01')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        status_code,
        k,
        value,
        status_code_avg_2h,
        clean_host,
        MAX(status_code_avg_2h) OVER (PARTITION BY k ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_code_avg_2h_max_30m,
        MAX(status_code) OVER (PARTITION BY k, host) AS k_max_status_code
    FROM filtered
    WHERE status_code_avg_2h >= 356
),
with_windows_app_logs AS (
    -- Running and moving aggregates
    SELECT
        value,
        status_code_avg_2h,
        clean_host,
        status_code_avg_2h_max_30m,
        k_max_status_code,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY k), 4) AS status_code_share,
        DENSE_RANK() OVER w_recent AS status_code_avg_2h_max_30m_dense_rank
    FROM with_windows
    WINDOW w_recent AS (ORDER BY status_code_avg_2h_max_30m DESC)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN status_code_share >= 0.81 THEN 'LARGE' ELSE 'SMALL' END AS status_code_share_tier,
    PERCENTILE_APPROX(status_code_avg_2h_max_30m_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS status_code_avg_2h_max_30m_dense_rank_quartiles,
    PERCENTILE_APPROX(status_code_avg_2h_max_30m, ARRAY(0.25, 0.5, 0.75)) AS status_code_avg_2h_max_30m_quartiles,
    AVG(k_max_status_code) AS avg_k_max_status_code
FROM with_windows_app_logs
GROUP BY 1
ORDER BY status_code_share_tier
