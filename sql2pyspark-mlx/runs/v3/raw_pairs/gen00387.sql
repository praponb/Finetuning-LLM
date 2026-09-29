SELECT
    id,
    city,
    rating,
    ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS rn
FROM suppliers;
