WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, value
),
flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        value,
        CAST((status_code & 2) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
),
base AS (
    -- Keep recent, valid rows
    SELECT
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        value,
        is_flagged,
        ROUND(latency_ms, 0) latency_ms_rounded,
        SUBSTRING(service, 1, 2) service_prefix
    FROM flattened_attrs
    WHERE log_ts >= DATE '2025-06-01'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY level ORDER BY latency_ms DESC) latency_ms_cume_dist,
        RANK() OVER (ORDER BY is_flagged DESC NULLS LAST) is_flagged_rank,
        RANK() OVER (PARTITION BY service, level ORDER BY is_flagged ASC NULLS FIRST) is_flagged_rank_2
    FROM base
),
base_app_logs AS (
    -- Basic cleansing and defaults
    SELECT *, TO_DATE(log_ts) log_ts_date
    FROM with_windows
    WHERE log_ts > TO_TIMESTAMP('2025-06-01 04:00:00')
)
-- Ranking and gap features
SELECT
    service,
    latency_ms,
    latency_ms_rounded,
    service_prefix,
    latency_ms_cume_dist,
    is_flagged_rank,
    log_ts_date,
    RANK() OVER (PARTITION BY service ORDER BY latency_ms_rounded ASC) latency_ms_rounded_rank,
    DENSE_RANK() OVER (ORDER BY latency_ms_cume_dist DESC NULLS LAST) latency_ms_cume_dist_dense_rank,
    SUM(latency_ms_rounded) OVER (PARTITION BY service) service_sum_latency_ms_rounded
FROM base_app_logs
WHERE latency_ms IS NOT NULL;
