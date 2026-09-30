WITH flattened AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, v
),
metrics AS (
    SELECT
        *,
        COUNT(status_code) OVER (PARTITION BY k) AS k_count_status_code,
        SUM(status_code) OVER (ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_code_sum_30m,
        CUME_DIST() OVER (PARTITION BY k ORDER BY status_code DESC) AS status_code_cume_dist
    FROM flattened
),
base AS (
    SELECT
        *,
        TO_DATE(log_ts) AS log_ts_date,
        CASE WHEN k_count_status_code >= 14 THEN 'High' WHEN k_count_status_code >= 3 THEN 'Medium' ELSE 'Low' END AS k_count_status_code_band
    FROM metrics
    WHERE log_ts > DATE '2025-06-01'
),
metrics_app_logs AS (
    SELECT
        *,
        MAX(status_code_sum_30m) OVER (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_code_sum_30m_max_30m,
        PERCENT_RANK() OVER (PARTITION BY host, k ORDER BY status_code_cume_dist DESC) AS status_code_cume_dist_percent_rank,
        SUM(status_code) OVER (PARTITION BY host) AS host_sum_status_code,
        ROUND(k_count_status_code / SUM(k_count_status_code) OVER (PARTITION BY service), 4) AS k_count_status_code_share,
        COUNT(status_code_sum_30m) OVER (PARTITION BY TO_DATE(log_ts), host ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_code_sum_30m_count_30m
    FROM base
),
with_windows AS (
    SELECT
        id,
        service,
        log_ts,
        k,
        status_code_sum_30m,
        k_count_status_code_band,
        status_code_sum_30m_max_30m,
        status_code_cume_dist_percent_rank,
        host_sum_status_code,
        status_code_sum_30m_count_30m,
        AVG(status_code_cume_dist_percent_rank) OVER (PARTITION BY TO_DATE(log_ts) ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_code_cume_dist_percent_rank_avg_1h,
        RANK() OVER (PARTITION BY k ORDER BY status_code DESC) AS status_code_rank,
        DENSE_RANK() OVER (PARTITION BY host ORDER BY status_code_sum_30m DESC) AS status_code_sum_30m_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('WEEK', log_ts), k ORDER BY status_code_cume_dist DESC) AS status_code_cume_dist_percent_rank_2
    FROM metrics_app_logs
)
SELECT
    log_ts,
    host_sum_status_code,
    status_code_sum_30m_count_30m,
    DENSE_RANK() OVER (PARTITION BY k ORDER BY status_code_sum_30m_dense_rank ASC) AS status_code_sum_30m_dense_rank_dense_rank,
    DENSE_RANK() OVER (PARTITION BY k ORDER BY status_code_sum_30m_dense_rank DESC NULLS FIRST) AS status_code_sum_30m_dense_rank_dense_rank_2,
    COUNT(status_code_sum_30m_count_30m) OVER (PARTITION BY service, k ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_code_sum_30m_count_30m_count_2h
FROM with_windows
