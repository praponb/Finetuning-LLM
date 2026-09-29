WITH enriched AS (
    SELECT
        *,
        SUM(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft,
        PERCENT_RANK() OVER (PARTITION BY city ORDER BY sqft DESC NULLS LAST) AS sqft_percent_rank,
        NTILE(10) OVER (PARTITION BY region ORDER BY sqft DESC, id) AS sqft_bucket,
        DENSE_RANK() OVER (PARTITION BY YEAR(opened_date), region ORDER BY sqft DESC) AS sqft_dense_rank,
        ROW_NUMBER() OVER w_grp AS event_seq
    FROM stores
    WHERE store_name IS NOT NULL
    WINDOW w_grp AS (PARTITION BY region ORDER BY opened_date, id)
),
enriched_stores AS (
    SELECT
        *,
        MIN(sqft_bucket) OVER w_running AS last3_min_sqft_bucket,
        MAX(sqft_bucket) OVER w_running AS last3_max_sqft_bucket,
        SUM(sqft_bucket) OVER w_running AS last3_sum_sqft_bucket,
        AVG(sqft_bucket) OVER w_running AS last3_avg_sqft_bucket,
        LAST_VALUE(region) IGNORE NULLS OVER w_part AS last_known_region
    FROM enriched
    WINDOW w_running AS (PARTITION BY region ORDER BY event_seq, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_part AS (ORDER BY sqft_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
filtered AS (
    SELECT *, CONCAT_WS('-', city, region) AS city_region_key
    FROM enriched_stores
    WHERE opened_date > DATE '2023-06-11'
      AND region IS NOT NULL
),
base AS (
    SELECT
        id,
        store_name,
        city,
        region,
        sqft_dense_rank,
        event_seq,
        last3_sum_sqft_bucket,
        sqft_percent_rank + cumulative_sqft AS sqft_percent_rank_plus_cumulative_sqft,
        DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date,
        COALESCE(region, 'OTHER') AS region_clean
    FROM filtered
    WHERE opened_date > DATE '2023-05-21'
)
SELECT
    CASE WHEN sqft_percent_rank_plus_cumulative_sqft >= 63 THEN 'HIGH' ELSE 'LOW' END AS sqft_percent_rank_plus_cumulative_sqft_tier,
    COUNT(CASE WHEN region = 'East' THEN 1 END) AS east_count,
    ROUND(AVG(last3_sum_sqft_bucket), 2) AS avg_last3_sum_sqft_bucket,
    ROUND(SUM(sqft_dense_rank) / NULLIF(COUNT(CASE WHEN city = 'Singapore' THEN 1 END), 0), 2) AS sqft_dense_rank_per_singapore
FROM base
GROUP BY CASE WHEN sqft_percent_rank_plus_cumulative_sqft >= 63 THEN 'HIGH' ELSE 'LOW' END
ORDER BY avg_last3_sum_sqft_bucket DESC NULLS FIRST;
