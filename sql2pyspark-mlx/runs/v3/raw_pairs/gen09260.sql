SELECT
    id,
    country,
    rating,
    LEAD(rating, 1) OVER (PARTITION BY country ORDER BY rating, id) AS next_rating
FROM suppliers
WHERE rating < 3.3;
