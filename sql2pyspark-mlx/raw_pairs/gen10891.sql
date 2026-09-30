WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS k, value
)
-- Running and moving aggregates
SELECT
    level,
    latency_ms,
    status_code,
    k,
    RANK() OVER (PARTITION BY level ORDER BY status_code) AS status_code_rank,
    RANK() OVER (PARTITION BY k ORDER BY latency_ms DESC) AS latency_ms_rank
FROM lines
WHERE service <> 'checkout'
