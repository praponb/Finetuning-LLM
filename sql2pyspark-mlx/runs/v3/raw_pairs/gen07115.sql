WITH SupplierTotals AS (
    SELECT supplier_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(category))) AS category_values, AVG(price) AS avg_price
    FROM products
    GROUP BY supplier_id
)
SELECT
    suppliers.id,
    suppliers.supplier_name,
    SupplierTotals.category_values,
    SupplierTotals.avg_price
FROM suppliers
LEFT OUTER JOIN SupplierTotals ON suppliers.id = SupplierTotals.supplier_id
ORDER BY suppliers.supplier_name, SupplierTotals.category_values
