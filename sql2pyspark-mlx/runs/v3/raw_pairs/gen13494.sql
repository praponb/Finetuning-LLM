WITH ranked_suppliers AS (
    SELECT *, RANK() OVER (PARTITION BY city ORDER BY rating DESC) AS rn
    FROM suppliers
)
SELECT *
FROM ranked_suppliers
WHERE rn <= 5;
