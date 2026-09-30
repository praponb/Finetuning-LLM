WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        latency_ms,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, v
)
-- Running and moving aggregates
SELECT
    host,
    latency_ms,
    k,
    v,
    ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY k, host), 4) AS latency_ms_share,
    PERCENT_RANK() OVER (ORDER BY latency_ms NULLS FIRST) AS latency_ms_percent_rank,
    ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY host), 4) AS latency_ms_share_2
FROM lines
