WITH SupplierTotals AS (
    SELECT supplier_id, SUM(CASE WHEN category = 'Home' THEN 1 ELSE 0 END) AS home_count, SUM(CASE WHEN brand = 'Stark' THEN cost ELSE 0 END) AS stark_cost
    FROM products
    GROUP BY supplier_id
)
SELECT
    suppliers.id,
    suppliers.country,
    suppliers.rating,
    SupplierTotals.home_count,
    SupplierTotals.stark_cost
FROM suppliers
LEFT JOIN SupplierTotals ON suppliers.id = SupplierTotals.supplier_id
ORDER BY home_count DESC, suppliers.id DESC;
