SELECT
    id,
    city,
    rating,
    NTILE(10) OVER (PARTITION BY city ORDER BY rating, id) AS bucket
FROM suppliers;
