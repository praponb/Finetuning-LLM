WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        latency_ms,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS attr_key, v
)
-- Aggregate with conditional metrics
SELECT
    attr_key,
    host,
    PERCENTILE_APPROX(latency_ms, ARRAY(0.25, 0.5, 0.75)) AS latency_ms_quartiles,
    COUNT(*) FILTER (WHERE host = 'h2') AS h2_rows
FROM exploded
GROUP BY attr_key, host
