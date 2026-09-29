SELECT
    id,
    city,
    rating,
    country,
    LEAD(rating, 1) OVER (PARTITION BY city ORDER BY rating, id) AS next_rating,
    ROUND(rating / SUM(rating) OVER (PARTITION BY city, country), 3) AS rating_share
FROM suppliers
ORDER BY rating_share DESC
