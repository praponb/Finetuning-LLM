WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(sqft) OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft,
        LAST_VALUE(region) IGNORE NULLS OVER (PARTITION BY region, city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_region
    FROM stores
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY opened_date NULLS FIRST, id) AS rank_in_group
    FROM enriched
    QUALIFY rank_in_group = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(cumulative_sqft, 1) OVER (PARTITION BY region, city ORDER BY cumulative_sqft, id) AS prev_cumulative_sqft,
        AVG(sqft) OVER (ORDER BY rank_in_group, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS sqft_avg_6_prior,
        NTILE(4) OVER (PARTITION BY city ORDER BY sqft DESC, id) AS sqft_bucket
    FROM ranked
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY sqft_bucket NULLS FIRST, id) AS latest_rank
    FROM with_windows
    QUALIFY latest_rank = 1
),
enriched_stores AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        cumulative_sqft,
        last_known_region,
        rank_in_group,
        prev_cumulative_sqft,
        sqft_avg_6_prior,
        sqft_bucket,
        latest_rank,
        LAG(sqft_bucket, 1) OVER (PARTITION BY city ORDER BY cumulative_sqft, id) AS prev_sqft_bucket,
        ROUND(sqft_avg_6_prior / SUM(sqft_avg_6_prior) OVER (PARTITION BY city), 4) AS sqft_avg_6_prior_share
    FROM latest
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        city,
        region,
        sqft,
        rank_in_group,
        sqft_avg_6_prior,
        sqft_bucket,
        latest_rank,
        prev_sqft_bucket,
        sqft_avg_6_prior_share,
        cumulative_sqft * sqft_avg_6_prior_share AS cumulative_sqft_times_sqft_avg_6_prior_share,
        COALESCE(region, 'UNKNOWN') AS region_clean
    FROM enriched_stores
    WHERE opened_date >= DATE '2023-09-19'
      AND cumulative_sqft BETWEEN 60445 AND 65416
)
-- Aggregate with conditional metrics
SELECT
    region,
    SUM(sqft_bucket) FILTER (WHERE region = 'West') AS west_total_sqft_bucket,
    SUM(CASE WHEN region = 'North' THEN sqft_avg_6_prior_share ELSE 0 END) AS north_sqft_avg_6_prior_share
FROM cleaned
GROUP BY region
ORDER BY west_total_sqft_bucket NULLS LAST;
