WITH SupplierTotals AS (
    SELECT supplier_id, SUM(CASE WHEN category = 'Fashion' THEN cost ELSE 0 END) AS fashion_cost
    FROM products
    GROUP BY supplier_id
)
SELECT s.rating, S.fashion_cost
FROM suppliers s
LEFT JOIN SupplierTotals S ON s.id = S.supplier_id;
