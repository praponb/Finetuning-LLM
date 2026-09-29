WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS k, attr_value
)
-- Window metrics over several different partitions
SELECT
    id,
    latency_ms,
    status_code,
    RANK() OVER w_hist AS status_code_rank
FROM exploded
WHERE latency_ms > 1152
WINDOW w_hist AS (PARTITION BY k, host ORDER BY status_code DESC);
