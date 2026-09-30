SELECT
    id,
    city,
    rating,
    SUM(rating) OVER (PARTITION BY city ORDER BY rating, id) AS running_rating
FROM suppliers
WHERE rating > 3.1
ORDER BY city ASC, running_rating DESC;
