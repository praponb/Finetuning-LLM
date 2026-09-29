WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, v
),
parsed AS (
    -- Semi-structured parsing
    SELECT
        id,
        log_ts,
        latency_ms,
        CAST((status_code & 4) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
    WHERE log_ts < '2023-11-01'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(has_fault) OVER (ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS has_fault_avg_1h,
        COUNT(has_fault) OVER w_running AS has_fault_count_30m,
        STDDEV_SAMP(has_fault) OVER w_running_2 AS log_ts_month_stddev_samp_has_fault,
        DENSE_RANK() OVER (ORDER BY status_code_high ASC) AS status_code_high_dense_rank
    FROM parsed
    WINDOW w_running AS (ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW), w_running_2 AS (PARTITION BY MONTH(log_ts))
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(has_fault, 0) AS has_fault_filled
    FROM metrics
    WHERE log_ts > TIMESTAMP '2025-06-01 12:00:00'
)
SELECT
    id,
    has_fault_count_30m,
    log_ts_month_stddev_samp_has_fault
FROM filtered
ORDER BY log_ts_month_stddev_samp_has_fault DESC NULLS LAST;
