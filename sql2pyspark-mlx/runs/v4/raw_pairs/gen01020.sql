WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        latency_ms,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, v
)
-- Running and moving aggregates
SELECT
    *,
    STDDEV_SAMP(latency_ms) OVER (PARTITION BY service) AS service_stddev_samp_latency_ms,
    AVG(latency_ms) OVER (PARTITION BY host ORDER BY latency_ms, id, k ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS latency_ms_avg_2_prior,
    CUME_DIST() OVER (PARTITION BY k ORDER BY latency_ms DESC NULLS FIRST) AS latency_ms_cume_dist
FROM flattened;
