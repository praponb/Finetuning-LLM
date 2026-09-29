WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) AS rn
    FROM suppliers
)
SELECT *
FROM ranked
WHERE rn <= 3;
