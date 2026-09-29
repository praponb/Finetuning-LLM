SELECT
    id,
    city,
    rating,
    COUNT(*) OVER (PARTITION BY city) AS city_count,
    LEAD(rating, 1) OVER (ORDER BY rating, id) AS next_rating
FROM suppliers
ORDER BY 1 DESC
