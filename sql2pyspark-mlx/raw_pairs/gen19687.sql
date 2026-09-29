WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, v
)
-- Window metrics over several different partitions
SELECT
    id,
    log_ts,
    status_code,
    k,
    LEAD(status_code) OVER w_ordered AS next_status_code,
    LAG(latency_ms) OVER w_ordered AS prev_latency_ms,
    LEAD(latency_ms) OVER w_ordered AS next_latency_ms,
    ROW_NUMBER() OVER w_ordered AS event_seq,
    LEAD(status_code, 1) OVER (PARTITION BY level ORDER BY latency_ms, id, k) AS next_status_code_2
FROM flattened
WINDOW w_ordered AS (PARTITION BY k, level ORDER BY log_ts, id, k)
