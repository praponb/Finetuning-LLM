WITH enriched AS (
    SELECT
        *,
        AVG(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS sqft_avg_3_prior,
        AVG(sqft) OVER (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS sqft_avg_5,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city), 4) AS sqft_share,
        NTILE(10) OVER (ORDER BY sqft DESC, id) AS sqft_bucket,
        COUNT(sqft) OVER (PARTITION BY region) AS region_count_sqft
    FROM stores
)
SELECT
    id,
    city,
    opened_date,
    sqft_avg_3_prior,
    sqft_avg_5,
    sqft_share,
    region_count_sqft
FROM enriched
