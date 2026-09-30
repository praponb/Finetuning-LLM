WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS k, value
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(status_code) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_code_max_2h,
        DENSE_RANK() OVER (PARTITION BY k ORDER BY status_code DESC) AS status_code_dense_rank,
        MAX(status_code) OVER (PARTITION BY level, service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS status_code_max_15m
    FROM lines
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(status_code_dense_rank) OVER (PARTITION BY level, k ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_code_dense_rank_avg_1h,
        SUM(status_code_max_15m) OVER (PARTITION BY level ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_code_max_15m_sum_1h,
        STDDEV_SAMP(status_code_max_2h) OVER (PARTITION BY service) AS service_stddev_samp_status_code_max_2h,
        AVG(status_code) OVER (PARTITION BY level) AS level_avg_status_code
    FROM with_windows
),
with_windows_app_logs AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(status_code_max_15m) OVER (PARTITION BY level) AS level_avg_status_code_max_15m,
        COUNT(status_code_max_15m) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_code_max_15m_count_30m,
        PERCENT_RANK() OVER (ORDER BY status_code_max_15m_sum_1h DESC) AS status_code_max_15m_sum_1h_percent_rank
    FROM metrics
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        status_code_max_15m * status_code_max_15m_sum_1h AS status_code_max_15m_times_status_code_max_15m_sum_1h,
        FLOOR(service_stddev_samp_status_code_max_2h) AS service_stddev_samp_status_code_max_2h_whole
    FROM with_windows_app_logs
    WHERE k IS NOT NULL
),
valid_app_logs AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(status_code_dense_rank_avg_1h / status_code_max_15m, 2) AS status_code_dense_rank_avg_1h_per_status_code_max_15m,
        DATE_FORMAT(log_ts, 'MMM yyyy') AS log_ts_label
    FROM valid
    WHERE log_ts > DATE '2025-06-01'
)
SELECT
    id,
    log_ts,
    k,
    value,
    status_code_dense_rank,
    status_code_dense_rank_avg_1h,
    status_code_max_15m_sum_1h,
    service_stddev_samp_status_code_max_2h,
    level_avg_status_code
FROM valid_app_logs
