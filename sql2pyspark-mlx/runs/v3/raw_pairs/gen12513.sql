SELECT
    id,
    region,
    city,
    opened_date,
    sqft,
    sqft - LAG(sqft, 1, 0) OVER (PARTITION BY region, city ORDER BY opened_date, id) AS sqft_change,
    COUNT(*) OVER (PARTITION BY city) AS city_count
FROM stores
ORDER BY opened_date ASC
