WITH RankedSuppliers AS (
    SELECT
        id,
        city,
        rating,
        country,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS rn
    FROM suppliers
)
SELECT
    id,
    city,
    rating,
    country
FROM RankedSuppliers
WHERE rn = 1
ORDER BY 3 DESC
