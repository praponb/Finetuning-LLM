WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        level,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, v
)
-- Running and moving aggregates
SELECT
    id,
    level,
    key,
    v,
    CUME_DIST() OVER w_recent status_code_cume_dist,
    CORR(status_code, latency_ms) OVER (PARTITION BY key) corr_status_code_latency_ms,
    CORR(status_code, latency_ms) OVER w_grp corr_status_code_latency_ms_2
FROM flattened
WINDOW w_recent AS (ORDER BY status_code), w_grp AS (PARTITION BY key, level)
