SELECT
    id,
    country,
    rating,
    city,
    ROUND(rating / SUM(rating) OVER (PARTITION BY country), 3) AS rating_share,
    ROUND(rating / SUM(rating) OVER (PARTITION BY city, country), 3) AS rating_share_2
FROM suppliers
ORDER BY rating_share DESC;
