WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, value
)
-- Ranking and gap features
SELECT
    *,
    PERCENT_RANK() OVER (PARTITION BY service ORDER BY latency_ms) AS latency_ms_percent_rank,
    FIRST_VALUE(k, TRUE) OVER (PARTITION BY host ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_k,
    NTILE(4) OVER (ORDER BY latency_ms, id, k) AS latency_ms_bucket,
    FIRST_VALUE(latency_ms) IGNORE NULLS OVER (PARTITION BY host ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_latency_ms,
    NTILE(10) OVER (PARTITION BY k ORDER BY latency_ms DESC, id, k) AS latency_ms_bucket_2
FROM exploded
