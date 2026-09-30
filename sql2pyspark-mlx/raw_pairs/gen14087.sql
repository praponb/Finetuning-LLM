WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, v
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY status_code DESC) AS status_code_cume_dist,
        AVG(status_code) OVER (PARTITION BY level, host) AS level_avg_status_code,
        SUM(status_code) OVER (PARTITION BY service) AS service_sum_status_code,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY host), 4) AS status_code_share,
        MAX(status_code) OVER (PARTITION BY service, level ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_code_max_30m
    FROM flattened
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(status_code_max_30m) OVER (PARTITION BY service) AS service_max_status_code_max_30m,
        CUME_DIST() OVER (ORDER BY status_code_cume_dist) AS status_code_cume_dist_cume_dist,
        MAX(status_code) OVER (PARTITION BY TO_DATE(log_ts), key) AS log_ts_to_date_max_status_code,
        MAX(status_code) OVER (PARTITION BY DATE_TRUNC('MONTH', log_ts), service) AS log_ts_period_max_status_code,
        STDDEV_SAMP(level_avg_status_code) OVER (PARTITION BY MONTH(log_ts), level) AS log_ts_month_stddev_samp_level_avg_status_code
    FROM enriched
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        key,
        v,
        status_code_cume_dist,
        service_sum_status_code,
        service_max_status_code_max_30m,
        log_ts_to_date_max_status_code,
        log_ts_period_max_status_code,
        log_ts_to_date_max_status_code * status_code_share AS log_ts_to_date_max_status_code_times_status_code_share,
        COALESCE(key, 'N/A') AS key_clean
    FROM with_windows
    WHERE log_ts > DATE '2025-06-01'
      AND key IS NOT NULL
)
SELECT
    id,
    service,
    log_ts,
    level,
    status_code,
    key,
    v,
    service_sum_status_code,
    log_ts_period_max_status_code,
    CASE WHEN key RLIKE '^(trace|version)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS key_zone
FROM cleaned
ORDER BY key DESC, log_ts
