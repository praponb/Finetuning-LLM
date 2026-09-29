SELECT
    id,
    city,
    rating,
    AVG(rating) OVER (PARTITION BY city) AS city_avg_rating
FROM suppliers
WHERE supplier_name LIKE '%Pacifi%'
ORDER BY city
