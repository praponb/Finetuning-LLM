SELECT
    id,
    city,
    rating,
    NTILE(4) OVER (PARTITION BY city ORDER BY rating, id) AS rating_ntile,
    rating - LAG(rating, 1) OVER (PARTITION BY city ORDER BY rating, id) AS rating_change
FROM suppliers
ORDER BY city, rating_change;
