WITH ranked AS (
    SELECT
        id,
        city,
        rating,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) rn
    FROM suppliers
)
SELECT id, city, rating
FROM ranked
WHERE rn <= 5
ORDER BY city DESC
