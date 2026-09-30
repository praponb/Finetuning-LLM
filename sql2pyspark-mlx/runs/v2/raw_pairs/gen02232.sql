SELECT
    id,
    country,
    rating,
    NTILE(3) OVER (PARTITION BY country ORDER BY rating, id) AS rating_ntile
FROM suppliers;
