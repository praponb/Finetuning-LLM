SELECT
    id,
    city,
    rating,
    country,
    ROUND(rating / SUM(rating) OVER (PARTITION BY city), 3) AS rating_share,
    FIRST_VALUE(city) OVER (PARTITION BY country ORDER BY rating DESC, id) AS first_city
FROM suppliers
ORDER BY city
