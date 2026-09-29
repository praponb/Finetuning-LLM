WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, attr_value
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        host,
        RANK() OVER (PARTITION BY attr_key ORDER BY latency_ms DESC) AS latency_ms_rank,
        DENSE_RANK() OVER (PARTITION BY host, level ORDER BY latency_ms DESC NULLS FIRST) AS latency_ms_dense_rank,
        CUME_DIST() OVER (PARTITION BY attr_key ORDER BY latency_ms NULLS LAST) AS latency_ms_cume_dist
    FROM flattened
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, latency_ms_dense_rank - latency_ms_cume_dist AS latency_ms_dense_rank_minus_latency_ms_cume_dist
    FROM metrics
    WHERE host = 'h3'
),
cleaned_app_logs AS (
    -- Keep recent, valid rows
    SELECT *, latency_ms_rank * latency_ms_dense_rank AS latency_ms_rank_times_latency_ms_dense_rank
    FROM cleaned
    WHERE latency_ms_dense_rank = 5
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN latency_ms_cume_dist >= 0.62 THEN 'HIGH' ELSE 'LOW' END AS latency_ms_cume_dist_tier,
    APPROX_PERCENTILE(latency_ms_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS latency_ms_cume_dist_quartiles,
    COUNT(CASE WHEN host = 'h2' THEN 1 END) AS h2_count,
    SIZE(COLLECT_SET(host)) AS num_hosts
FROM cleaned_app_logs
GROUP BY 1;
