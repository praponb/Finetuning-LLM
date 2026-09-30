WITH exploded AS (
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, v
),
parsed AS (
    SELECT
        id,
        latency_ms,
        status_code,
        k,
        v,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 2) != 0 AS INT) AS has_fault
    FROM exploded
),
valid AS (
    SELECT
        id,
        status_code,
        k,
        v,
        status_code_high,
        has_fault,
        UPPER(k) AS k_upper,
        ROUND(has_fault * 0.85, 2) AS has_fault_adjusted
    FROM parsed
    WHERE k IS NOT NULL
),
with_windows AS (
    SELECT
        id,
        status_code,
        k,
        v,
        status_code_high,
        has_fault,
        k_upper,
        has_fault_adjusted,
        SUM(has_fault_adjusted) OVER (PARTITION BY k) AS k_sum_has_fault_adjusted,
        STDDEV_SAMP(status_code_high) OVER (PARTITION BY k) AS k_stddev_samp_status_code_high,
        CUME_DIST() OVER (ORDER BY has_fault_adjusted DESC) AS has_fault_adjusted_cume_dist
    FROM valid
),
metrics AS (
    SELECT
        id,
        status_code,
        k,
        v,
        status_code_high,
        k_upper,
        MIN(has_fault) OVER (PARTITION BY k) AS k_min_has_fault,
        MAX(has_fault) OVER (PARTITION BY k) AS k_max_has_fault,
        PERCENT_RANK() OVER (PARTITION BY k ORDER BY has_fault DESC) AS has_fault_percent_rank,
        CUME_DIST() OVER (PARTITION BY k ORDER BY has_fault) AS has_fault_cume_dist
    FROM with_windows
),
with_windows_app_logs AS (
    SELECT *, CUME_DIST() OVER (PARTITION BY k ORDER BY k_max_has_fault DESC) AS k_max_has_fault_cume_dist
    FROM metrics
    WHERE k_max_has_fault >= 0
),
scored AS (
    SELECT
        id,
        status_code,
        k,
        k_upper,
        k_min_has_fault,
        k_max_has_fault,
        has_fault_cume_dist,
        k_max_has_fault_cume_dist,
        COUNT(has_fault_percent_rank) OVER w_hist AS k_count_has_fault_percent_rank
    FROM with_windows_app_logs
    WINDOW w_hist AS (PARTITION BY k)
)
SELECT
    id,
    k,
    k_min_has_fault,
    k_max_has_fault,
    has_fault_cume_dist,
    k_count_has_fault_percent_rank
FROM scored
ORDER BY k_max_has_fault DESC NULLS FIRST
