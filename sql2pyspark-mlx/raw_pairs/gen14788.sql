WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, attr_value
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        CASE WHEN host IN ('h2', 'h3') THEN 'Core' ELSE 'Other' END AS host_group,
        DATEDIFF(CURRENT_DATE(), log_ts) AS days_since_log_ts
    FROM lines
    WHERE key IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(days_since_log_ts) OVER (PARTITION BY host) AS host_max_days_since_log_ts,
        MAX(days_since_log_ts) OVER (PARTITION BY host ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS days_since_log_ts_max_1h,
        COUNT(days_since_log_ts) OVER (PARTITION BY host ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS days_since_log_ts_count_30m,
        RANK() OVER (PARTITION BY host ORDER BY days_since_log_ts DESC NULLS FIRST) AS days_since_log_ts_rank,
        RANK() OVER (PARTITION BY MONTH(log_ts), level ORDER BY days_since_log_ts DESC NULLS FIRST) AS days_since_log_ts_rank_2
    FROM valid
    WHERE host <> 'h2'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(days_since_log_ts_rank) OVER (PARTITION BY level) AS level_min_days_since_log_ts_rank,
        PERCENT_RANK() OVER (ORDER BY days_since_log_ts_count_30m DESC) AS days_since_log_ts_count_30m_percent_rank,
        MAX(days_since_log_ts_max_1h) OVER (ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS days_since_log_ts_max_1h_max_30m,
        COUNT(host_max_days_since_log_ts) OVER (PARTITION BY service, host ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS host_max_days_since_log_ts_count_30m,
        MAX(days_since_log_ts_rank_2) OVER (ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS days_since_log_ts_rank_2_max_30m
    FROM metrics
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        service,
        level,
        status_code,
        host_group,
        days_since_log_ts_max_1h,
        days_since_log_ts_rank,
        days_since_log_ts_count_30m_percent_rank,
        days_since_log_ts_max_1h_max_30m,
        host_max_days_since_log_ts_count_30m,
        days_since_log_ts_rank_2_max_30m,
        SUM(status_code) OVER (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_code_sum_30m,
        STDDEV_SAMP(status_code) OVER (PARTITION BY host) AS host_stddev_samp_status_code,
        CUME_DIST() OVER (PARTITION BY TO_DATE(log_ts), level ORDER BY host_max_days_since_log_ts DESC) AS host_max_days_since_log_ts_cume_dist
    FROM scored
)
-- Aggregate with conditional metrics
SELECT
    service,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    BIT_OR(status_code) AS status_code_bit_or,
    COUNT(DISTINCT level) AS distinct_levels,
    COUNT(DISTINCT service) AS distinct_services
FROM with_windows
GROUP BY service
