WITH supplier_stats AS (
    SELECT supplier_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(brand))) AS brand_values
    FROM products
    WHERE cost BETWEEN 330 AND 340
    GROUP BY supplier_id
)
SELECT suppliers.id, suppliers.rating, supplier_stats.brand_values
FROM suppliers
INNER JOIN supplier_stats ON suppliers.id = supplier_stats.supplier_id
ORDER BY supplier_stats.brand_values ASC, rating DESC
