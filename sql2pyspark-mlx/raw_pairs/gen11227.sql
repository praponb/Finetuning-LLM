SELECT
    id,
    country,
    rating,
    LEAD(rating, 2, 0) OVER (PARTITION BY country ORDER BY rating, id) next_rating
FROM suppliers
ORDER BY rating ASC, country DESC
