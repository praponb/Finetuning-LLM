WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS attr_key, value
)
-- Running and moving aggregates
SELECT
    id,
    service,
    host,
    latency_ms,
    attr_key,
    value,
    LEAD(status_code) OVER w_recent AS next_status_code,
    LAG(status_code) OVER w_recent AS prev_status_code,
    LEAD(latency_ms) OVER w_recent AS next_latency_ms,
    LAG(latency_ms) OVER w_recent AS prev_latency_ms,
    ROW_NUMBER() OVER w_recent AS seq_no
FROM flattened
WHERE latency_ms <= 1485
WINDOW w_recent AS (PARTITION BY host, attr_key ORDER BY latency_ms, id, attr_key);
