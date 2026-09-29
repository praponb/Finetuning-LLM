WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, attr_value
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY level ORDER BY latency_ms NULLS LAST) AS latency_ms_dense_rank,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY service), 4) AS latency_ms_share
    FROM exploded
    WHERE level IN ('ERROR', 'INFO', 'DEBUG')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        service,
        level,
        latency_ms,
        latency_ms_dense_rank,
        latency_ms_share,
        COUNT(latency_ms_share) OVER (PARTITION BY attr_key) AS attr_key_count_latency_ms_share,
        AVG(latency_ms_dense_rank) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS latency_ms_dense_rank_avg_1h,
        COUNT(latency_ms_share) OVER (PARTITION BY host, service) AS host_count_latency_ms_share
    FROM enriched
),
enriched_app_logs AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (ORDER BY latency_ms_dense_rank DESC) AS latency_ms_dense_rank_rank,
        CUME_DIST() OVER (ORDER BY latency_ms_share DESC NULLS FIRST) AS latency_ms_share_cume_dist,
        ROUND(latency_ms_dense_rank_avg_1h / SUM(latency_ms_dense_rank_avg_1h) OVER (PARTITION BY level, service), 4) AS latency_ms_dense_rank_avg_1h_share,
        STDDEV_SAMP(attr_key_count_latency_ms_share) OVER (PARTITION BY service) AS service_stddev_samp_attr_key_count_latency_ms_share,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY service, level), 4) AS latency_ms_share_2
    FROM with_windows
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT service
    FROM enriched_app_logs
    INTERSECT
    SELECT service
    FROM app_logs
    WHERE level = 'INFO'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched_app_logs s
    JOIN remaining_keys k ON s.service = k.service
),
remaining_keys_2 AS (
    -- Set difference on keys
    SELECT id, service
    FROM verified
    EXCEPT ALL
    SELECT id, service
    FROM app_logs
    WHERE host IN ('h2', 'h4', 'h3')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM verified s
    JOIN remaining_keys_2 k ON s.id = k.id AND s.service = k.service
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        service,
        level,
        latency_ms_dense_rank,
        attr_key_count_latency_ms_share,
        latency_ms_dense_rank_avg_1h,
        host_count_latency_ms_share,
        latency_ms_dense_rank_rank,
        latency_ms_share_cume_dist,
        latency_ms_dense_rank_avg_1h_share,
        service_stddev_samp_attr_key_count_latency_ms_share,
        latency_ms_share_2,
        COALESCE(latency_ms_dense_rank_rank, 0) AS latency_ms_dense_rank_rank_filled
    FROM reconciled
    WHERE latency_ms_share_2 >= 0.21
)
SELECT
    service,
    level,
    latency_ms_dense_rank,
    attr_key_count_latency_ms_share,
    latency_ms_dense_rank_avg_1h,
    host_count_latency_ms_share,
    latency_ms_dense_rank_rank,
    latency_ms_share_cume_dist,
    latency_ms_share_2
FROM valid
