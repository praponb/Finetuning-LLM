SELECT
    id,
    country,
    rating,
    city,
    SUM(rating) OVER (PARTITION BY country ORDER BY rating, id) running_rating,
    ROUND(rating / SUM(rating) OVER (PARTITION BY city), 3) rating_share
FROM suppliers;
