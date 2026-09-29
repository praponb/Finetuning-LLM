SELECT
    id,
    city,
    sqft,
    region,
    AVG(sqft) OVER (PARTITION BY city) city_avg_sqft,
    COUNT(*) OVER (PARTITION BY region) region_count
FROM stores
ORDER BY city
