SELECT *
FROM (
    SELECT
        id,
        city,
        rating,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) AS rn
    FROM suppliers
) AS ranked
WHERE rn <= 3
ORDER BY id DESC;
