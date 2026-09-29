WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, v
),
enriched AS (
    -- Ranking and gap features
    SELECT *, MAX(status_code) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS status_code_max_15m
    FROM lines
    WHERE log_ts >= '2023-04-01'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT *, RANK() OVER (PARTITION BY MONTH(log_ts) ORDER BY status_code_max_15m NULLS FIRST) AS status_code_max_15m_rank
    FROM enriched
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(status_code_max_15m / SUM(status_code_max_15m) OVER (PARTITION BY key, host), 4) AS status_code_max_15m_share,
        AVG(status_code_max_15m) OVER (PARTITION BY level, key) AS level_avg_status_code_max_15m,
        AVG(status_code_max_15m_rank) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_code_max_15m_rank_avg_1h,
        CUME_DIST() OVER (ORDER BY status_code_max_15m_rank DESC) AS status_code_max_15m_rank_cume_dist
    FROM with_windows
),
enriched_app_logs AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(status_code_max_15m_rank) OVER (PARTITION BY key) AS key_avg_status_code_max_15m_rank,
        SUM(status_code_max_15m_rank) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_code_max_15m_rank_sum_2h
    FROM metrics
)
SELECT
    host,
    log_ts,
    status_code_max_15m_share,
    level_avg_status_code_max_15m,
    status_code_max_15m_rank_avg_1h,
    status_code_max_15m_rank_sum_2h,
    CASE WHEN host RLIKE '^(h3|h4)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS host_zone
FROM enriched_app_logs
ORDER BY status_code_max_15m_rank_avg_1h DESC NULLS FIRST
