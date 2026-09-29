SELECT
    id,
    country,
    rating,
    MAX(rating) OVER (PARTITION BY country) AS country_max_rating,
    RANK() OVER (PARTITION BY country ORDER BY rating) AS rating_rank
FROM suppliers
WHERE rating IS NOT NULL
