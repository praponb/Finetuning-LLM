SELECT
    id,
    city,
    sqft,
    region,
    SUM(sqft) OVER (PARTITION BY city) AS city_sum_sqft,
    MAX(sqft) OVER (PARTITION BY region) AS region_max_sqft
FROM stores
WHERE city = 'Tokyo';
