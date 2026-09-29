SELECT
    id,
    country,
    rating,
    LEAD(rating, 2) OVER (PARTITION BY country ORDER BY rating, id) AS next_rating
FROM suppliers
