WITH SupplierStats AS (
    SELECT supplier_id, SORT_ARRAY(COLLECT_SET(category)) AS category_list, COUNT(CASE WHEN category = 'Fashion' THEN 1 END) AS fashion_count
    FROM products
    WHERE cost > 440
    GROUP BY supplier_id
)
SELECT s.supplier_name, S.category_list, S.fashion_count
FROM suppliers s
LEFT JOIN SupplierStats S ON s.id = S.supplier_id
WHERE S.fashion_count >= 6
ORDER BY s.supplier_name
