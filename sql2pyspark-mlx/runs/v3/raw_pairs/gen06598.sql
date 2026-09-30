SELECT
    id,
    city,
    rating,
    country,
    LAG(rating, 2) OVER (PARTITION BY city ORDER BY rating, id) AS prev_rating,
    SUM(rating) OVER (PARTITION BY country, city ORDER BY rating, id) AS running_rating
FROM suppliers
ORDER BY prev_rating DESC
