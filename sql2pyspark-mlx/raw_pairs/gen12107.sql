WITH lines AS (
    SELECT
        id,
        host,
        level,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, value
),
scored AS (
    SELECT *, AVG(status_code) OVER (PARTITION BY level) level_avg_status_code
    FROM lines
),
kept_keys AS (
    SELECT level, host
    FROM scored
    EXCEPT
    SELECT level, host
    FROM app_logs
    WHERE host IN ('h3', 'h2', 'h1')
),
verified AS (
    SELECT s.*
    FROM scored s
    INNER JOIN kept_keys k ON s.level = k.level AND s.host = k.host
),
valid AS (
    SELECT
        *,
        CASE WHEN host IN ('h3', 'h1') THEN 'Group A' ELSE 'Other' END host_group,
        COALESCE(k, 'DIRECT') k_clean
    FROM verified
    WHERE k IS DISTINCT FROM 'trace'
),
with_windows AS (
    SELECT
        id,
        level,
        latency_ms,
        k,
        value,
        host_group,
        k_clean,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY level), 4) latency_ms_share,
        COUNT(level_avg_status_code) OVER (PARTITION BY level, host) level_count_level_avg_status_code
    FROM valid
)
SELECT
    CASE WHEN latency_ms_share >= 0.65 THEN 'HIGH' ELSE 'LOW' END AS latency_ms_share_tier,
    k,
    SUM(CASE WHEN level = 'DEBUG' THEN latency_ms ELSE 0 END) AS debug_latency_ms,
    PERCENTILE_APPROX(latency_ms_share, ARRAY(0.25, 0.5, 0.75)) AS latency_ms_share_quartiles,
    ROUND(AVG(level_count_level_avg_status_code), 2) AS avg_level_count_level_avg_status_code,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    SIZE(COLLECT_SET(k)) AS num_ks,
    SORT_ARRAY(COLLECT_SET(k)) AS k_list
FROM with_windows
GROUP BY 1, 2
