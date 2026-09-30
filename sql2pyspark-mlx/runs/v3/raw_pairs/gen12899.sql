SELECT
    id,
    region,
    sqft,
    city,
    PERCENT_RANK() OVER (PARTITION BY region ORDER BY sqft) AS sqft_pct_rank,
    MAX(sqft) OVER (PARTITION BY city) AS city_max_sqft
FROM stores
WHERE store_name LIKE '%Air%';
