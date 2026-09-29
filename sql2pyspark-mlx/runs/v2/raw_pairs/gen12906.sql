WITH SupplierTotals AS (
    SELECT supplier_id, COUNT(cost) AS cost_count, AVG(price) AS avg_price
    FROM products
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.supplier_name,
    S.cost_count,
    S.avg_price
FROM suppliers s
INNER JOIN SupplierTotals S ON s.id = S.supplier_id
ORDER BY cost_count DESC, s.supplier_name, id
LIMIT 5;
