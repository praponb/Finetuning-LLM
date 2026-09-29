WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, value
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY service), 4) AS latency_ms_share,
        AVG(latency_ms) OVER w_ordered AS log_ts_period_avg_latency_ms
    FROM exploded
    WHERE latency_ms >= 2286
    WINDOW w_ordered AS (PARTITION BY DATE_TRUNC('WEEK', log_ts), key)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        log_ts,
        latency_ms,
        key,
        value,
        latency_ms_share,
        log_ts_period_avg_latency_ms,
        COUNT(log_ts_period_avg_latency_ms) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS log_ts_period_avg_latency_ms_count_1h,
        COUNT(latency_ms) OVER (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS latency_ms_count_15m,
        MAX(log_ts_period_avg_latency_ms) OVER (PARTITION BY service) AS service_max_log_ts_period_avg_latency_ms,
        AVG(log_ts_period_avg_latency_ms) OVER (PARTITION BY key) AS key_avg_log_ts_period_avg_latency_ms
    FROM with_windows
),
scored AS (
    -- Running and moving aggregates
    SELECT
        log_ts,
        value,
        latency_ms_share,
        log_ts_period_avg_latency_ms_count_1h,
        latency_ms_count_15m,
        service_max_log_ts_period_avg_latency_ms,
        ROUND(log_ts_period_avg_latency_ms / SUM(log_ts_period_avg_latency_ms) OVER (PARTITION BY MONTH(log_ts), key), 4) AS log_ts_period_avg_latency_ms_share,
        PERCENT_RANK() OVER (PARTITION BY key ORDER BY latency_ms_share NULLS FIRST) AS latency_ms_share_percent_rank
    FROM metrics
)
SELECT
    value,
    latency_ms_count_15m,
    log_ts_period_avg_latency_ms_share,
    latency_ms_share_percent_rank
FROM scored;
