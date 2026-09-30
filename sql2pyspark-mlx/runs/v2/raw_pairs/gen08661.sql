SELECT
    id,
    country,
    rating,
    MIN(rating) OVER (PARTITION BY country) AS country_min_rating,
    LEAD(rating, 1) OVER (ORDER BY rating, id) AS next_rating
FROM suppliers;
