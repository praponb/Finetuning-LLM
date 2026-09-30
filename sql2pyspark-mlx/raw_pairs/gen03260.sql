WITH lines AS (
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
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, value
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT *, COUNT(latency_ms) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS latency_ms_count_30m
    FROM lines
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(status_code) OVER (PARTITION BY host) AS host_count_status_code,
        COUNT(latency_ms_count_30m) OVER w_part AS latency_ms_count_30m_count_2h,
        CORR(latency_ms, status_code) OVER w_ordered AS corr_latency_ms_status_code,
        SUM(latency_ms) OVER w_recent AS latency_ms_sum_15m,
        DENSE_RANK() OVER (PARTITION BY service ORDER BY status_code) AS status_code_dense_rank
    FROM metrics
    WHERE status_code < 441
    WINDOW w_part AS (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY YEAR(log_ts), service), w_recent AS (PARTITION BY host ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW)
),
with_windows_app_logs AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY status_code DESC) AS status_code_cume_dist,
        RANK() OVER (PARTITION BY level, attr_key ORDER BY latency_ms_sum_15m DESC) AS latency_ms_sum_15m_rank
    FROM with_windows
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        service,
        host,
        status_code,
        attr_key,
        value,
        host_count_status_code,
        latency_ms_count_30m_count_2h,
        latency_ms_sum_15m,
        status_code_dense_rank,
        status_code_cume_dist,
        ROUND(latency_ms_sum_15m_rank * 0.85, 2) AS adjusted_latency_ms_sum_15m_rank,
        GREATEST(latency_ms_count_30m, latency_ms) AS greatest_latency_ms_count_30m_latency_ms,
        COALESCE(attr_key, 'NONE') AS attr_key_filled
    FROM with_windows_app_logs
    WHERE log_ts >= TO_TIMESTAMP('2025-06-01 11:00:00')
      AND attr_key IS NOT NULL
),
base AS (
    -- Keep recent, valid rows
    SELECT
        service,
        attr_key,
        value,
        host_count_status_code,
        latency_ms_count_30m_count_2h,
        latency_ms_sum_15m,
        status_code_cume_dist,
        greatest_latency_ms_count_30m_latency_ms,
        TRIM(host) AS clean_host
    FROM cleaned
    WHERE attr_key IS NOT NULL
)
-- Aggregate with conditional metrics
SELECT
    service,
    AVG(host_count_status_code) AS avg_host_count_status_code,
    COUNT_IF(service = 'billing') AS billing_events,
    APPROX_PERCENTILE(latency_ms_sum_15m, 0.9) AS latency_ms_sum_15m_p90,
    PERCENTILE_APPROX(status_code_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS status_code_cume_dist_quartiles,
    COUNT(DISTINCT attr_key) AS distinct_attr_keys,
    PERCENTILE_APPROX(latency_ms_count_30m_count_2h, ARRAY(0.25, 0.5, 0.75)) AS latency_ms_count_30m_count_2h_quartiles
FROM base
GROUP BY service
HAVING AVG(host_count_status_code) > 0
