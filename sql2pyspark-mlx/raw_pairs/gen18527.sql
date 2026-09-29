WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN region = 'East' THEN 1 ELSE 0 END AS is_east,
        COALESCE(region, 'OTHER') AS resolved_region
    FROM stores
    WHERE region IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(is_east) OVER (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS is_east_avg_3,
        ROW_NUMBER() OVER (PARTITION BY YEAR(opened_date), region ORDER BY opened_date, id) AS event_seq,
        LAST_VALUE(is_east, TRUE) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_is_east,
        NTILE(5) OVER (PARTITION BY region ORDER BY sqft DESC, id) AS sqft_bucket
    FROM base
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        store_name,
        city,
        is_east,
        resolved_region,
        event_seq,
        last_known_is_east,
        sqft_bucket
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY event_seq DESC NULLS LAST, id) = 1
),
metrics_stores AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        resolved_region,
        last_known_is_east,
        sqft_bucket,
        LAG(is_east) OVER (PARTITION BY city ORDER BY last_known_is_east, id) AS prev_is_east,
        LAG(sqft_bucket) OVER (PARTITION BY city ORDER BY last_known_is_east, id) AS prev_sqft_bucket,
        LEAD(sqft_bucket) OVER (PARTITION BY city ORDER BY last_known_is_east, id) AS next_sqft_bucket,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY last_known_is_east, id) AS row_seq
    FROM latest
    WHERE last_known_is_east BETWEEN 62 AND 63
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY last_known_is_east NULLS LAST) AS last_known_is_east_cume_dist,
        AVG(prev_sqft_bucket) OVER (ORDER BY prev_is_east, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS prev_sqft_bucket_avg_3,
        SUM(prev_sqft_bucket) OVER (ORDER BY prev_is_east, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prev_sqft_bucket
    FROM metrics_stores
)
SELECT
    id,
    resolved_region,
    last_known_is_east,
    sqft_bucket,
    prev_is_east,
    prev_sqft_bucket,
    next_sqft_bucket,
    row_seq,
    prev_sqft_bucket_avg_3
FROM enriched
