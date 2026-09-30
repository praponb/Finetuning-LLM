WITH ranked_suppliers AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY city ORDER BY rating DESC) AS pos
    FROM suppliers
)
SELECT *
FROM ranked_suppliers
WHERE pos <= 2;
