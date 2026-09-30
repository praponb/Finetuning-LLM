WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        latency_ms,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, attr_value
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        attr_value,
        ROUND(latency_ms, 2) AS latency_ms_rounded,
        CASE WHEN host IN ('h4', 'h3') THEN 'Group A' ELSE 'Other' END AS host_group
    FROM flattened
    WHERE attr_key IS NOT NULL
      AND host IN ('h2', 'h4', 'h3')
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER w_recent AS latency_ms_rounded_dense_rank,
        DENSE_RANK() OVER (ORDER BY latency_ms_rounded DESC NULLS FIRST) AS latency_ms_rounded_dense_rank_2,
        CUME_DIST() OVER (ORDER BY latency_ms_rounded DESC NULLS LAST) AS latency_ms_rounded_cume_dist
    FROM valid
    WINDOW w_recent AS (ORDER BY latency_ms_rounded)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER w_recent AS latency_ms_rounded_dense_rank_dense_rank,
        CUME_DIST() OVER (ORDER BY latency_ms_rounded_dense_rank DESC NULLS FIRST) AS latency_ms_rounded_dense_rank_cume_dist
    FROM scored
    WINDOW w_recent AS (ORDER BY latency_ms_rounded_dense_rank NULLS LAST)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY latency_ms_rounded DESC NULLS FIRST) AS latency_ms_rounded_percent_rank,
        PERCENT_RANK() OVER (ORDER BY latency_ms_rounded_dense_rank_dense_rank DESC NULLS LAST) AS latency_ms_rounded_dense_rank_dense_rank_percent_rank
    FROM metrics
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        attr_value,
        latency_ms_rounded,
        host_group,
        latency_ms_rounded_dense_rank,
        latency_ms_rounded_dense_rank_2,
        latency_ms_rounded_dense_rank_dense_rank,
        latency_ms_rounded_dense_rank_cume_dist,
        latency_ms_rounded_percent_rank,
        latency_ms_rounded_dense_rank_dense_rank_percent_rank,
        CEIL(latency_ms_rounded_dense_rank_cume_dist) AS latency_ms_rounded_dense_rank_cume_dist_whole,
        LEAST(latency_ms_rounded_dense_rank_dense_rank, latency_ms_rounded) AS least_latency_ms_rounded_dense_rank_dense_rank_latency_ms_rounded
    FROM enriched
    WHERE latency_ms_rounded IS NOT NULL
),
valid_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        id,
        attr_value,
        latency_ms_rounded,
        host_group,
        latency_ms_rounded_dense_rank_2,
        latency_ms_rounded_dense_rank_dense_rank,
        latency_ms_rounded_percent_rank,
        latency_ms_rounded_dense_rank_cume_dist_whole,
        least_latency_ms_rounded_dense_rank_dense_rank_latency_ms_rounded,
        latency_ms_rounded_dense_rank_2 / NULLIF(latency_ms_rounded_dense_rank_dense_rank_percent_rank, 0) AS latency_ms_rounded_dense_rank_2_per_latency_ms_rounded_dense_rank_dense_rank_percent_rank,
        ROUND(latency_ms_rounded_percent_rank, 1) AS latency_ms_rounded_percent_rank_rounded
    FROM cleaned
    WHERE latency_ms_rounded_dense_rank_2 > 8
)
SELECT
    id,
    attr_value,
    latency_ms_rounded,
    host_group,
    latency_ms_rounded_dense_rank_2,
    latency_ms_rounded_dense_rank_dense_rank,
    latency_ms_rounded_dense_rank_cume_dist_whole,
    latency_ms_rounded_dense_rank_2_per_latency_ms_rounded_dense_rank_dense_rank_percent_rank,
    latency_ms_rounded_percent_rank_rounded
FROM valid_app_logs
ORDER BY latency_ms_rounded_dense_rank_cume_dist_whole NULLS FIRST, latency_ms_rounded DESC
