WITH flattened AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, value
),
enriched AS (
    SELECT
        *,
        COUNT(latency_ms) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS latency_ms_count_15m,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY key), 4) AS latency_ms_share,
        SUM(latency_ms) OVER w_part AS latency_ms_sum_2h
    FROM flattened
    WHERE level = 'INFO'
    WINDOW w_part AS (PARTITION BY MONTH(log_ts), service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW)
),
base AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        key,
        value,
        latency_ms_count_15m,
        latency_ms_sum_2h,
        CAST(latency_ms_sum_2h AS STRING) AS latency_ms_sum_2h_str,
        latency_ms_sum_2h - latency_ms_count_15m AS latency_ms_sum_2h_minus_latency_ms_count_15m
    FROM enriched
    WHERE log_ts >= TO_DATE('2025-06-01')
      AND key IS NOT NULL
),
scored AS (
    SELECT
        id,
        log_ts,
        level,
        latency_ms_count_15m,
        latency_ms_sum_2h,
        latency_ms_sum_2h_str,
        PERCENT_RANK() OVER (PARTITION BY level, key ORDER BY latency_ms DESC NULLS LAST) AS latency_ms_percent_rank,
        CUME_DIST() OVER (PARTITION BY level ORDER BY latency_ms_sum_2h_minus_latency_ms_count_15m NULLS LAST) AS latency_ms_sum_2h_minus_latency_ms_count_15m_cume_dist,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY level), 4) AS latency_ms_share
    FROM base
),
valid AS (
    SELECT
        *,
        COALESCE(latency_ms_percent_rank, 0) AS latency_ms_percent_rank_filled,
        SUBSTRING(level, 1, 3) AS level_prefix
    FROM scored
    WHERE latency_ms_percent_rank > 0.24
)
SELECT
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    ROUND(SUM(latency_ms_sum_2h) / NULLIF(COUNT(CASE WHEN level = 'WARN' THEN 1 END), 0), 2) AS latency_ms_sum_2h_per_warn,
    COUNT_IF(level = 'DEBUG') AS debug_events,
    (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS duration_min,
    GROUPING(DATE_TRUNC('DAY', log_ts)) AS grouping_id
FROM valid
GROUP BY CUBE (DATE_TRUNC('DAY', log_ts))
ORDER BY debug_events;
