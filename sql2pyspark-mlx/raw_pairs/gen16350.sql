WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, v
),
unpacked AS (
    -- Semi-structured parsing
    SELECT
        log_ts,
        status_code,
        v,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 4) != 0 AS INT) AS flag_bit4
    FROM lines
    WHERE log_ts < '2023-06-23'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        log_ts,
        status_code,
        v,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY TO_DATE(log_ts)), 4) AS status_code_share,
        DENSE_RANK() OVER (ORDER BY status_code_high NULLS LAST) AS status_code_high_dense_rank
    FROM unpacked
),
metrics_app_logs AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(status_code_high_dense_rank) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_code_high_dense_rank_sum_1h,
        DENSE_RANK() OVER (ORDER BY status_code_share DESC) AS status_code_share_dense_rank
    FROM metrics
)
-- Multi-level aggregation
SELECT
    CASE WHEN status_code_high_dense_rank_sum_1h >= 33 THEN 'HIGH' ELSE 'LOW' END AS status_code_high_dense_rank_sum_1h_tier,
    MAX(log_ts) AS last_seen,
    ROUND(AVG(status_code_high_dense_rank), 2) AS avg_status_code_high_dense_rank,
    APPROX_PERCENTILE(status_code_share_dense_rank, 0.9) AS status_code_share_dense_rank_p90
FROM metrics_app_logs
GROUP BY status_code_high_dense_rank_sum_1h_tier
HAVING avg_status_code_high_dense_rank > 0;
