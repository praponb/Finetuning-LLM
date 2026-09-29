WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, attr_value
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(latency_ms) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS latency_ms_avg_15m,
        CORR(latency_ms, status_code) OVER w_part AS corr_latency_ms_status_code
    FROM lines
    WINDOW w_part AS (PARTITION BY level)
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        key,
        attr_value,
        corr_latency_ms_status_code,
        CAST(status_code AS DOUBLE) AS status_code_num,
        COALESCE(key, 'DIRECT') AS key_clean
    FROM with_windows
    WHERE corr_latency_ms_status_code > 0.0
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(corr_latency_ms_status_code) OVER w_ordered AS log_ts_period_sum_corr_latency_ms_status_code,
        MAX(corr_latency_ms_status_code) OVER (PARTITION BY level, key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS corr_latency_ms_status_code_max_1h,
        MAX(status_code_num) OVER w_running AS host_max_status_code_num
    FROM base
    WINDOW w_ordered AS (PARTITION BY DATE_TRUNC('month', log_ts), key), w_running AS (PARTITION BY host)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        log_ts,
        key,
        corr_latency_ms_status_code,
        key_clean,
        log_ts_period_sum_corr_latency_ms_status_code,
        host_max_status_code_num,
        DENSE_RANK() OVER (PARTITION BY level ORDER BY corr_latency_ms_status_code_max_1h DESC NULLS LAST) AS corr_latency_ms_status_code_max_1h_dense_rank
    FROM enriched
)
SELECT
    log_ts,
    key,
    corr_latency_ms_status_code,
    key_clean,
    log_ts_period_sum_corr_latency_ms_status_code,
    host_max_status_code_num,
    corr_latency_ms_status_code_max_1h_dense_rank,
    CASE WHEN key RLIKE '^(version|trace)$' THEN 'EXTENDED' WHEN key RLIKE '^(region)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS key_zone
FROM scored
ORDER BY log_ts_period_sum_corr_latency_ms_status_code, log_ts DESC;
