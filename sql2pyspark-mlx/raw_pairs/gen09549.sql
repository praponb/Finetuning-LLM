WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS k, value
)
SELECT
    id,
    log_ts,
    level,
    latency_ms,
    k,
    value
FROM lines
ORDER BY log_ts, k;
