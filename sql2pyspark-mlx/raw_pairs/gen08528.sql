WITH lines AS (
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, attr_value
),
kept_keys AS (
    SELECT id
    FROM lines
    EXCEPT
    SELECT id
    FROM app_logs
    WHERE host IN ('h1', 'h4')
),
verified AS (
    SELECT s.*
    FROM lines s
    JOIN kept_keys k ON s.id = k.id
),
metrics AS (
    SELECT
        id,
        host,
        latency_ms,
        MIN(latency_ms) OVER (PARTITION BY key) key_min_latency_ms,
        COUNT(latency_ms) OVER (PARTITION BY MONTH(log_ts), key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) latency_ms_count_30m
    FROM verified
),
with_windows AS (
    SELECT
        id,
        host,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY host), 4) latency_ms_share,
        MAX(key_min_latency_ms) OVER (PARTITION BY host) host_max_key_min_latency_ms,
        ROUND(key_min_latency_ms / SUM(key_min_latency_ms) OVER (PARTITION BY host), 4) key_min_latency_ms_share,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY host), 4) latency_ms_share_2
    FROM metrics
)
SELECT
    CASE WHEN latency_ms_share_2 >= 0.67 THEN 'HIGH' ELSE 'LOW' END AS latency_ms_share_2_tier,
    host,
    SUM(CASE WHEN host = 'h1' THEN latency_ms_share ELSE 0 END) AS h1_latency_ms_share,
    MAX(latency_ms_share) FILTER (WHERE host = 'h3') AS h3_max_latency_ms_share,
    APPROX_PERCENTILE(latency_ms_share_2, ARRAY(0.25, 0.5, 0.75)) AS latency_ms_share_2_quartiles,
    COUNT(*) AS event_count
FROM with_windows
GROUP BY CASE WHEN latency_ms_share_2 >= 0.67 THEN 'HIGH' ELSE 'LOW' END, host
