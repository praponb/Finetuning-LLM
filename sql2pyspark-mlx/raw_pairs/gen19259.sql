SELECT
    id,
    country,
    city,
    rating,
    RANK() OVER (PARTITION BY country, city ORDER BY rating) AS rating_rank
FROM suppliers
WHERE rating <= 3.3
ORDER BY rating
