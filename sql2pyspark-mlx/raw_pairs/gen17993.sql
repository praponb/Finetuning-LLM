SELECT
    id,
    country,
    rating,
    MIN(rating) OVER (PARTITION BY country) AS country_min_rating
FROM suppliers
