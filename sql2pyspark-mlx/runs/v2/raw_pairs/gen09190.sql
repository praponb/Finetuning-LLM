SELECT
    id,
    country,
    rating,
    MAX(rating) OVER (PARTITION BY country) AS country_max_rating,
    LEAD(rating, 1) OVER (ORDER BY rating, id) AS next_rating
FROM suppliers;
