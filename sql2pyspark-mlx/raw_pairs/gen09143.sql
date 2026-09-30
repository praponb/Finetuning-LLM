WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, attr_value
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(status_code) OVER (PARTITION BY DATE_TRUNC('YEAR', log_ts), attr_key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) status_code_sum_1h,
        AVG(status_code) OVER (PARTITION BY YEAR(log_ts), attr_key) log_ts_year_avg_status_code,
        CORR(latency_ms, status_code) OVER (PARTITION BY YEAR(log_ts)) corr_latency_ms_status_code,
        CORR(status_code, latency_ms) OVER (PARTITION BY attr_key, service) corr_status_code_latency_ms,
        AVG(latency_ms) OVER (PARTITION BY DATE_TRUNC('WEEK', log_ts), level ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) latency_ms_avg_15m
    FROM lines
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        attr_value,
        status_code_sum_1h,
        log_ts_year_avg_status_code,
        corr_status_code_latency_ms,
        latency_ms_avg_15m,
        CASE WHEN service IN ('checkout', 'search') THEN 'Priority' ELSE 'Other' END service_group,
        CAST(log_ts AS DATE) log_ts_day
    FROM metrics
    WHERE log_ts >= TIMESTAMP '2025-06-01 12:00:00'
      AND corr_status_code_latency_ms >= -0.3
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        attr_value,
        status_code_sum_1h,
        log_ts_year_avg_status_code,
        service_group,
        COUNT(log_ts_year_avg_status_code) OVER (PARTITION BY service, attr_key) service_count_log_ts_year_avg_status_code,
        AVG(corr_status_code_latency_ms) OVER (PARTITION BY service, level ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) corr_status_code_latency_ms_avg_15m,
        COUNT(latency_ms_avg_15m) OVER (PARTITION BY level ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) latency_ms_avg_15m_count_1h
    FROM base
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        TRIM(service) clean_service,
        ROUND(latency_ms / service_count_log_ts_year_avg_status_code, 2) latency_ms_per_service_count_log_ts_year_avg_status_code
    FROM with_windows
    WHERE corr_status_code_latency_ms_avg_15m != 0
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        attr_value,
        log_ts_year_avg_status_code,
        service_group,
        service_count_log_ts_year_avg_status_code,
        corr_status_code_latency_ms_avg_15m,
        latency_ms_avg_15m_count_1h,
        latency_ms_per_service_count_log_ts_year_avg_status_code,
        FLOOR(log_ts_year_avg_status_code) log_ts_year_avg_status_code_whole
    FROM valid
    WHERE log_ts >= TO_DATE('2025-06-01')
),
with_windows_app_logs AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(log_ts_year_avg_status_code) OVER w_grp service_sum_log_ts_year_avg_status_code,
        COUNT(log_ts_year_avg_status_code_whole) OVER w_part log_ts_year_avg_status_code_whole_count_2h,
        ROUND(service_count_log_ts_year_avg_status_code / SUM(service_count_log_ts_year_avg_status_code) OVER (PARTITION BY level), 4) service_count_log_ts_year_avg_status_code_share,
        CUME_DIST() OVER (PARTITION BY DATE_TRUNC('YEAR', log_ts), service ORDER BY corr_status_code_latency_ms_avg_15m) corr_status_code_latency_ms_avg_15m_cume_dist,
        MIN(service_count_log_ts_year_avg_status_code) OVER w_grp service_min_service_count_log_ts_year_avg_status_code
    FROM filtered
    WHERE log_ts_year_avg_status_code = 269
    WINDOW w_grp AS (PARTITION BY service), w_part AS (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW)
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    level,
    metric,
    metric_value
FROM (
    SELECT
        level,
        service_count_log_ts_year_avg_status_code_share,
        latency_ms_per_service_count_log_ts_year_avg_status_code,
        corr_status_code_latency_ms_avg_15m_cume_dist
    FROM with_windows_app_logs
) src
UNPIVOT (
    metric_value FOR metric IN (service_count_log_ts_year_avg_status_code_share, latency_ms_per_service_count_log_ts_year_avg_status_code AS LATENCY_MS_PER_SERVICE_COUNT_LOG_TS_YEAR_AVG_STATUS_CODE_VAL, corr_status_code_latency_ms_avg_15m_cume_dist AS CORR_STATUS_CODE_LATENCY_MS_AVG_15M_CUME_DIST_VAL)
);
