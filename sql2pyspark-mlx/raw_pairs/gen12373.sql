WITH SupplierTotals AS (
    SELECT supplier_id, ROUND(AVG(cost), 2) AS avg_cost
    FROM products
    GROUP BY supplier_id
)
SELECT s.id, s.supplier_name, S.avg_cost
FROM suppliers s
JOIN SupplierTotals S ON s.id = S.supplier_id
WHERE S.avg_cost > 52
ORDER BY s.id DESC, S.avg_cost
LIMIT 50;
