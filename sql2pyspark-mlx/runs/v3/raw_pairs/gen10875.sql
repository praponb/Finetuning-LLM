SELECT
    id,
    country,
    rating,
    ROUND(rating / SUM(rating) OVER (PARTITION BY country), 3) AS rating_share
FROM suppliers
ORDER BY rating;
