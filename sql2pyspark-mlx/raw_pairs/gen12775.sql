WITH parsed AS (
    -- Higher-order array functions
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        payload_json,
        context,
        CAST((status_code & 2) != 0 AS INT) AS has_fault,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec,
        context['trace'] AS trace,
        ARRAY_CONTAINS(MAP_KEYS(context), 'trace') AS has_trace,
        context['region'] AS region
    FROM app_logs
)
-- Ranking and gap features
SELECT
    id,
    host,
    latency_ms,
    duration_sec,
    region,
    NTILE(5) OVER (PARTITION BY level ORDER BY has_fault, id) AS has_fault_bucket,
    SUM(duration_sec) OVER (PARTITION BY host) AS host_sum_duration_sec,
    ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY service, level), 4) AS latency_ms_share,
    SUM(latency_ms) OVER (PARTITION BY service ORDER BY has_fault, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_latency_ms
FROM parsed
WHERE service = 'checkout'
