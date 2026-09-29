WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        latency_ms,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, v
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        service,
        latency_ms,
        k,
        v,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY k), 4) AS latency_ms_share,
        MIN(latency_ms) OVER (PARTITION BY service, k) AS service_min_latency_ms,
        RANK() OVER (PARTITION BY k ORDER BY latency_ms DESC) AS latency_ms_rank,
        DENSE_RANK() OVER (PARTITION BY k, service ORDER BY latency_ms) AS latency_ms_dense_rank
    FROM exploded
),
kept_keys AS (
    -- Set difference on keys
    SELECT service, id
    FROM with_windows
    EXCEPT ALL
    SELECT service, id
    FROM app_logs
    WHERE log_ts < '2023-10-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys k ON s.service = k.service AND s.id = k.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY service ORDER BY latency_ms_dense_rank DESC NULLS FIRST) AS latency_ms_dense_rank_dense_rank,
        CUME_DIST() OVER (ORDER BY latency_ms_rank DESC NULLS LAST) AS latency_ms_rank_cume_dist
    FROM verified
),
with_windows_app_logs AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER w_ordered AS latency_ms_dense_rank_dense_rank_cume_dist,
        ROUND(service_min_latency_ms / SUM(service_min_latency_ms) OVER (PARTITION BY k, service), 4) AS service_min_latency_ms_share
    FROM metrics
    WINDOW w_ordered AS (ORDER BY latency_ms_dense_rank_dense_rank)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        service,
        latency_ms,
        k,
        v,
        latency_ms_share,
        service_min_latency_ms,
        latency_ms_rank,
        latency_ms_dense_rank,
        latency_ms_dense_rank_dense_rank,
        latency_ms_rank_cume_dist,
        latency_ms_dense_rank_dense_rank_cume_dist,
        service_min_latency_ms_share,
        COUNT(latency_ms_dense_rank_dense_rank) OVER (PARTITION BY service, k) AS service_count_latency_ms_dense_rank_dense_rank,
        AVG(latency_ms_rank_cume_dist) OVER (PARTITION BY service) AS service_avg_latency_ms_rank_cume_dist
    FROM with_windows_app_logs
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        service,
        k,
        service_min_latency_ms_share
    FROM enriched
) src
PIVOT (
    SUM(service_min_latency_ms_share) AS total
    FOR k IN ('region', 'trace')
);
