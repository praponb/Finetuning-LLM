WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, v
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        k,
        STDDEV_SAMP(status_code) OVER (PARTITION BY MONTH(log_ts), host) AS log_ts_month_stddev_samp_status_code,
        ROW_NUMBER() OVER (PARTITION BY k ORDER BY log_ts DESC, id, k) AS row_seq,
        MAX(status_code) OVER (PARTITION BY host ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_code_max_1h
    FROM exploded
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        k,
        log_ts_month_stddev_samp_status_code,
        status_code_max_1h,
        ROUND(status_code_max_1h / SUM(status_code_max_1h) OVER (PARTITION BY k), 4) AS status_code_max_1h_share,
        LAST_VALUE(log_ts_month_stddev_samp_status_code, TRUE) OVER (PARTITION BY k ORDER BY status_code_max_1h, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_log_ts_month_stddev_samp_status_code
    FROM metrics
    WHERE row_seq IS NOT NULL
),
metrics_app_logs AS (
    -- Running and moving aggregates
    SELECT
        id,
        k,
        status_code_max_1h_share,
        last_known_log_ts_month_stddev_samp_status_code,
        LAG(log_ts_month_stddev_samp_status_code, 1) OVER (PARTITION BY k ORDER BY last_known_log_ts_month_stddev_samp_status_code, id, k) AS prev_log_ts_month_stddev_samp_status_code,
        LAST_VALUE(last_known_log_ts_month_stddev_samp_status_code, TRUE) OVER (PARTITION BY k ORDER BY last_known_log_ts_month_stddev_samp_status_code, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last_known_log_ts_month_stddev_samp_status_code
    FROM with_windows
),
with_windows_app_logs AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        k,
        last_known_log_ts_month_stddev_samp_status_code,
        last_known_last_known_log_ts_month_stddev_samp_status_code,
        LEAD(status_code_max_1h_share, 2) OVER w_running AS next_status_code_max_1h_share,
        ROW_NUMBER() OVER w_running AS row_seq
    FROM metrics_app_logs
    WINDOW w_running AS (PARTITION BY k ORDER BY status_code_max_1h_share, id, k)
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY next_status_code_max_1h_share DESC, id, k) AS rank_in_group
    FROM with_windows_app_logs
    QUALIFY rank_in_group = 1
)
SELECT
    id,
    k,
    last_known_last_known_log_ts_month_stddev_samp_status_code,
    next_status_code_max_1h_share,
    rank_in_group,
    CASE WHEN k RLIKE '^(region|trace)$' THEN 'CORE' WHEN k RLIKE '^(version)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS k_zone
FROM ranked
ORDER BY last_known_last_known_log_ts_month_stddev_samp_status_code;
