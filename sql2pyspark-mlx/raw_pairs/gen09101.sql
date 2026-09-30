WITH SupplierSummary AS (
    SELECT supplier_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(category))) AS category_values
    FROM products
    GROUP BY supplier_id
)
SELECT s.rating, s.city, S.category_values
FROM suppliers s
LEFT JOIN SupplierSummary S ON s.id = S.supplier_id
ORDER BY s.rating DESC
