SELECT *
FROM (
    SELECT
        id,
        city,
        rating,
        country,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS rn
    FROM suppliers
) AS t
WHERE rn <= 3
ORDER BY country
