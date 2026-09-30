SELECT
    id,
    city,
    region,
    sqft,
    MIN(sqft) OVER (PARTITION BY city) AS city_min_sqft,
    ROUND(sqft / SUM(sqft) OVER (PARTITION BY region), 4) AS sqft_share,
    NTILE(4) OVER (PARTITION BY MONTH(opened_date) ORDER BY sqft, id) AS sqft_bucket,
    LEAD(sqft, 1) OVER (PARTITION BY region ORDER BY opened_date, id) AS next_sqft
FROM stores;
