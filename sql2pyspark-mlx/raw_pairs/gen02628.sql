WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT_WS('-', store_name, region) AS store_name_region_key
    FROM stores
    WHERE opened_date > DATE '2023-05-25'
      AND region IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(sqft) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS sqft_avg_5_prior,
        NTILE(4) OVER (PARTITION BY region ORDER BY sqft, id) AS sqft_bucket,
        NTILE(5) OVER (ORDER BY sqft DESC, id) AS sqft_bucket_2,
        LAST_VALUE(region) IGNORE NULLS OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_region
    FROM valid
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        store_name,
        sqft_bucket,
        sqft_bucket_2
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY sqft_bucket_2 DESC, id) = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        sqft_bucket,
        AVG(sqft_bucket_2) OVER (ORDER BY sqft_bucket_2, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS sqft_bucket_2_avg_5_prior,
        LAG(sqft_bucket, 1) OVER (ORDER BY sqft_bucket_2, id) AS prev_sqft_bucket
    FROM deduped
),
stats AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN sqft_bucket >= 9 THEN 'HIGH' ELSE 'LOW' END AS sqft_bucket_tier,
        ROUND(AVG(sqft_bucket_2_avg_5_prior), 2) AS avg_sqft_bucket_2_avg_5_prior,
        ROUND(AVG(prev_sqft_bucket), 2) AS avg_prev_sqft_bucket,
        ROUND(AVG(sqft_bucket_2_avg_5_prior), 2) AS avg_sqft_bucket_2_avg_5_prior_2
    FROM enriched
    GROUP BY sqft_bucket_tier
    HAVING avg_sqft_bucket_2_avg_5_prior > 0
)
SELECT
    sqft_bucket_tier,
    avg_sqft_bucket_2_avg_5_prior,
    avg_prev_sqft_bucket
FROM stats
ORDER BY avg_prev_sqft_bucket DESC NULLS FIRST, avg_sqft_bucket_2_avg_5_prior
