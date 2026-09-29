WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        level,
        latency_ms,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS attr_key, value
)
-- Window metrics over several different partitions
SELECT
    id,
    host,
    level,
    attr_key,
    AVG(latency_ms) OVER (PARTITION BY host ORDER BY latency_ms, id, attr_key ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS latency_ms_avg_3,
    ROW_NUMBER() OVER (PARTITION BY attr_key, host ORDER BY latency_ms DESC, id, attr_key) AS seq_no,
    NTILE(4) OVER (PARTITION BY attr_key ORDER BY latency_ms, id, attr_key) AS latency_ms_bucket
FROM exploded
WHERE host IN ('h3', 'h4', 'h2')
