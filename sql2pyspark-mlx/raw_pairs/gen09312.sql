WITH lines AS (
    SELECT
        id,
        service,
        log_ts,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS k, v
),
flattened_attrs AS (
    SELECT
        id,
        log_ts,
        status_code,
        k,
        CAST((status_code & 4) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
),
metrics AS (
    SELECT *, COUNT(status_code_high) OVER (PARTITION BY k ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_code_high_count_30m
    FROM flattened_attrs
    WHERE status_code >= 348
),
with_windows AS (
    SELECT
        has_fault,
        status_code_high,
        AVG(status_code_high_count_30m) OVER (PARTITION BY TO_DATE(log_ts), k) AS log_ts_to_date_avg_status_code_high_count_30m
    FROM metrics
),
valid AS (
    SELECT
        *,
        status_code_high * log_ts_to_date_avg_status_code_high_count_30m AS status_code_high_times_log_ts_to_date_avg_status_code_high_count_30m,
        ROUND(log_ts_to_date_avg_status_code_high_count_30m * 0.85, 2) AS log_ts_to_date_avg_status_code_high_count_30m_scaled
    FROM with_windows
    WHERE status_code_high = 1
)
SELECT
    CASE WHEN status_code_high_times_log_ts_to_date_avg_status_code_high_count_30m >= 79 THEN 'HIGH' ELSE 'LOW' END AS status_code_high_times_log_ts_to_date_avg_status_code_high_count_30m_tier,
    ROUND(AVG(status_code_high), 2) AS avg_status_code_high,
    ROUND(AVG(log_ts_to_date_avg_status_code_high_count_30m), 2) AS avg_log_ts_to_date_avg_status_code_high_count_30m,
    MAX(status_code_high_times_log_ts_to_date_avg_status_code_high_count_30m) AS max_status_code_high_times_log_ts_to_date_avg_status_code_high_count_30m
FROM valid
GROUP BY CASE WHEN status_code_high_times_log_ts_to_date_avg_status_code_high_count_30m >= 79 THEN 'HIGH' ELSE 'LOW' END
