SELECT
    id,
    city,
    rating,
    ROUND(rating / SUM(rating) OVER (PARTITION BY city), 3) AS rating_share
FROM suppliers;
