SELECT
    id,
    region,
    sqft,
    SUM(sqft) OVER (PARTITION BY region) AS region_sum_sqft
FROM stores
WHERE sqft > 6229
ORDER BY id DESC;
