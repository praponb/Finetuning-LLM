WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, value
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        service,
        host,
        level,
        latency_ms,
        key,
        LOWER(host) AS host_lower,
        ROUND(status_code * 0.85, 2) AS status_code_adjusted
    FROM exploded
    WHERE log_ts >= TIMESTAMP '2025-06-01 08:00:00'
      AND key IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY level ORDER BY latency_ms DESC NULLS FIRST) AS latency_ms_rank,
        MAX(latency_ms) OVER w_recent AS service_max_latency_ms,
        CUME_DIST() OVER (ORDER BY status_code_adjusted NULLS FIRST) AS status_code_adjusted_cume_dist,
        MAX(status_code_adjusted) OVER (PARTITION BY host, key) AS host_max_status_code_adjusted
    FROM valid
    WINDOW w_recent AS (PARTITION BY service)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        service,
        host,
        level,
        latency_ms,
        key,
        host_lower,
        status_code_adjusted,
        latency_ms_rank,
        service_max_latency_ms,
        status_code_adjusted_cume_dist,
        host_max_status_code_adjusted,
        CUME_DIST() OVER (PARTITION BY host ORDER BY status_code_adjusted_cume_dist DESC) AS status_code_adjusted_cume_dist_cume_dist,
        CUME_DIST() OVER (PARTITION BY service ORDER BY host_max_status_code_adjusted) AS host_max_status_code_adjusted_cume_dist,
        RANK() OVER (PARTITION BY service ORDER BY latency_ms_rank NULLS FIRST) AS latency_ms_rank_rank
    FROM scored
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER w_running AS status_code_adjusted_cume_dist_cume_dist_2,
        PERCENT_RANK() OVER (PARTITION BY service ORDER BY service_max_latency_ms DESC NULLS LAST) AS service_max_latency_ms_percent_rank,
        ROUND(service_max_latency_ms / SUM(service_max_latency_ms) OVER (PARTITION BY level), 4) AS service_max_latency_ms_share,
        PERCENT_RANK() OVER w_recent AS status_code_adjusted_cume_dist_cume_dist_percent_rank
    FROM with_windows
    WINDOW w_running AS (PARTITION BY key ORDER BY status_code_adjusted_cume_dist DESC NULLS LAST), w_recent AS (PARTITION BY key ORDER BY status_code_adjusted_cume_dist_cume_dist DESC NULLS LAST)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        service_max_latency_ms / NULLIF(service_max_latency_ms_percent_rank, 0) AS service_max_latency_ms_per_service_max_latency_ms_percent_rank,
        UPPER(level) AS level_upper
    FROM metrics
    WHERE key IS NOT NULL
)
-- Aggregate with conditional metrics
SELECT
    level,
    CASE WHEN status_code_adjusted >= 68 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_adjusted_tier,
    COUNT(*) FILTER (WHERE host = 'h2') AS h2_rows,
    SORT_ARRAY(COLLECT_SET(host)) AS host_list,
    MAX(status_code_adjusted) FILTER (WHERE service = 'billing') AS billing_max_status_code_adjusted,
    COUNT(DISTINCT level) AS distinct_levels,
    MAX(status_code_adjusted_cume_dist_cume_dist_2) FILTER (WHERE host = 'h3') AS h3_max_status_code_adjusted_cume_dist_cume_dist_2,
    GROUPING(level) AS grouping_level
FROM filtered
GROUP BY GROUPING SETS ((level, CASE WHEN status_code_adjusted >= 68 THEN 'PREMIUM' ELSE 'STANDARD' END), (level))
ORDER BY h3_max_status_code_adjusted_cume_dist_cume_dist_2 DESC NULLS LAST
