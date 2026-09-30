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
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, value
),
parsed AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        log_ts,
        level,
        latency_ms,
        k,
        CAST((status_code & 1) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
    WHERE latency_ms >= 2374
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        level,
        latency_ms,
        is_flagged,
        status_code_high,
        latency_ms + status_code_high AS latency_ms_plus_status_code_high,
        COALESCE(k, 'N/A') AS k_clean
    FROM parsed
    WHERE log_ts > DATE '2025-06-01'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, FLOOR(is_flagged) AS is_flagged_whole
    FROM valid
    WHERE latency_ms BETWEEN 1206 AND 2293
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(status_code_high) OVER (PARTITION BY level) AS level_max_status_code_high,
        CUME_DIST() OVER w_running AS status_code_high_cume_dist
    FROM cleaned
    WINDOW w_running AS (ORDER BY status_code_high DESC)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY level ORDER BY is_flagged_whole DESC) AS is_flagged_whole_percent_rank,
        MAX(latency_ms_plus_status_code_high) OVER (PARTITION BY level) AS level_max_latency_ms_plus_status_code_high
    FROM metrics
)
SELECT
    level,
    is_flagged,
    status_code_high,
    latency_ms_plus_status_code_high,
    k_clean,
    is_flagged_whole,
    level_max_status_code_high,
    is_flagged_whole_percent_rank,
    CASE WHEN level RLIKE '^(DEBUG|WARN)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS level_zone
FROM with_windows
ORDER BY level_max_status_code_high DESC
