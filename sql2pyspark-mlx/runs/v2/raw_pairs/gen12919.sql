WITH SupplierStats AS (
    SELECT supplier_id, COUNT(CASE WHEN category = 'Grocery' THEN 1 END) AS grocery_count, ROUND((100 * SUM(CASE WHEN brand = 'Globex' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_globex
    FROM products
    WHERE product_name NOT LIKE '%Bottl%'
    GROUP BY supplier_id
)
SELECT
    suppliers.id,
    suppliers.rating,
    SupplierStats.grocery_count,
    SupplierStats.pct_globex
FROM suppliers
JOIN SupplierStats ON suppliers.id = SupplierStats.supplier_id
WHERE SupplierStats.pct_globex >= 57;
