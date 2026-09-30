WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        latency_ms,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, attr_value
)
-- Running and moving aggregates
SELECT
    *,
    AVG(latency_ms) OVER (PARTITION BY k, host ORDER BY latency_ms, id, k ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS latency_ms_avg_2_prior,
    ROW_NUMBER() OVER (PARTITION BY host, service ORDER BY latency_ms, id, k) AS seq_no,
    AVG(latency_ms) OVER (PARTITION BY service ORDER BY latency_ms, id, k ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS latency_ms_avg_3,
    ROW_NUMBER() OVER (ORDER BY latency_ms DESC, id, k) AS row_seq
FROM exploded
