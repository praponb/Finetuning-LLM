SELECT
    id,
    city,
    sqft,
    AVG(sqft) OVER (PARTITION BY city) city_avg_sqft
FROM stores
WHERE store_name IS NOT NULL
ORDER BY city, 3 DESC;
