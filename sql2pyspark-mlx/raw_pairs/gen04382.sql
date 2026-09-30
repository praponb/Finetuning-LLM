WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, value
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN latency_ms >= 1875 THEN 'High' WHEN latency_ms >= 1039 THEN 'Medium' ELSE 'Low' END AS latency_ms_tier,
        COALESCE(k, 'OTHER') AS k_clean
    FROM flattened
    WHERE k IS NOT NULL
      AND log_ts >= TO_DATE('2025-06-01')
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(latency_ms) OVER (ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS latency_ms_avg_15m,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY level), 4) AS latency_ms_share,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY DATE_TRUNC('MONTH', log_ts), level), 4) AS latency_ms_share_2
    FROM valid
    WHERE log_ts >= '2023-09-01'
),
metrics_app_logs AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(latency_ms_share_2) OVER (PARTITION BY host) AS host_max_latency_ms_share_2,
        CUME_DIST() OVER (PARTITION BY TO_DATE(log_ts) ORDER BY latency_ms_share DESC NULLS FIRST) AS latency_ms_share_cume_dist,
        SUM(latency_ms_share) OVER (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS latency_ms_share_sum_1h
    FROM metrics
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN latency_ms_share_2 >= 0.72 THEN 'LARGE' ELSE 'SMALL' END AS latency_ms_share_2_tier,
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    CASE WHEN host_max_latency_ms_share_2 >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS host_max_latency_ms_share_2_tier,
    (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS span_minutes,
    (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS span_minutes_2,
    AVG(latency_ms_share_2) AS avg_latency_ms_share_2
FROM metrics_app_logs
GROUP BY latency_ms_share_2_tier, activity_day, host_max_latency_ms_share_2_tier
ORDER BY activity_day ASC
