WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, attr_value
),
parsed AS (
    -- Higher-order array functions
    SELECT
        id,
        log_ts,
        status_code,
        attr_value,
        CAST((status_code & 1) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
    WHERE status_code > 380
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, has_fault + status_code_high AS has_fault_plus_status_code_high
    FROM parsed
    WHERE log_ts < '2023-07-01'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(status_code) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_code_sum_1h,
        PERCENT_RANK() OVER (ORDER BY status_code_high) AS status_code_high_percent_rank,
        PERCENT_RANK() OVER (ORDER BY has_fault_plus_status_code_high DESC) AS has_fault_plus_status_code_high_percent_rank
    FROM filtered
),
filtered_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        id,
        attr_value,
        has_fault,
        status_code_sum_1h,
        status_code_high_percent_rank,
        has_fault_plus_status_code_high_percent_rank,
        has_fault_plus_status_code_high_percent_rank + status_code_sum_1h AS has_fault_plus_status_code_high_percent_rank_plus_status_code_sum_1h
    FROM with_windows
    WHERE status_code_sum_1h < 1701
)
SELECT
    id,
    status_code_high_percent_rank,
    has_fault_plus_status_code_high_percent_rank,
    has_fault_plus_status_code_high_percent_rank_plus_status_code_sum_1h
FROM filtered_app_logs;
