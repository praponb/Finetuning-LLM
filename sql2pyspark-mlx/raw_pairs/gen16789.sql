SELECT
    id,
    city,
    rating,
    MAX(rating) OVER (PARTITION BY city) AS city_max_rating
FROM suppliers
ORDER BY id DESC
