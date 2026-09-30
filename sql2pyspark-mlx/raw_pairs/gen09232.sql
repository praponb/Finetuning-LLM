WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, v
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        host,
        latency_ms,
        status_code,
        v,
        CUME_DIST() OVER (PARTITION BY attr_key, host ORDER BY latency_ms DESC NULLS LAST) AS latency_ms_cume_dist,
        COUNT(latency_ms) OVER (PARTITION BY YEAR(log_ts), host ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS latency_ms_count_15m,
        AVG(status_code) OVER (PARTITION BY host, attr_key) AS host_avg_status_code
    FROM lines
    WHERE latency_ms >= 1145
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SUBSTRING(host, 1, 2) AS host_prefix,
        CAST(status_code AS STRING) AS status_code_str
    FROM with_windows
    WHERE latency_ms_count_15m BETWEEN 7 AND 12
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CEIL(latency_ms_cume_dist) AS latency_ms_cume_dist_whole
    FROM filtered
    WHERE status_code = 278
),
filtered_app_logs AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN latency_ms >= 2089 THEN 'High' WHEN latency_ms >= 872 THEN 'Medium' ELSE 'Low' END AS latency_ms_tier,
        ROUND(latency_ms_count_15m * 0.85, 2) AS latency_ms_count_15m_adjusted
    FROM valid
    WHERE latency_ms_count_15m BETWEEN 5 AND 9
)
SELECT
    id,
    latency_ms,
    v,
    latency_ms_cume_dist,
    latency_ms_count_15m,
    host_avg_status_code,
    host_prefix,
    status_code_str,
    latency_ms_cume_dist_whole
FROM filtered_app_logs
ORDER BY id DESC NULLS FIRST;
