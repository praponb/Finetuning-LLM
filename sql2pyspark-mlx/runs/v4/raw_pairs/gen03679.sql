WITH flattened AS (
    SELECT
        id,
        service,
        host,
        latency_ms,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS key, value
)
SELECT
    *,
    ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY host), 4) AS latency_ms_share,
    PERCENT_RANK() OVER (PARTITION BY host, key ORDER BY latency_ms NULLS FIRST) AS latency_ms_percent_rank,
    ROW_NUMBER() OVER (PARTITION BY host ORDER BY latency_ms DESC, id, key) AS row_seq
FROM flattened
