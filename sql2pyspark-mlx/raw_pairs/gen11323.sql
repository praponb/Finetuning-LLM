WITH supplier_stats AS (
    SELECT supplier_id, SORT_ARRAY(COLLECT_SET(brand)) AS brand_list
    FROM products
    GROUP BY supplier_id
)
SELECT
    suppliers.id,
    suppliers.country,
    suppliers.rating,
    supplier_stats.brand_list
FROM suppliers
JOIN supplier_stats ON suppliers.id = supplier_stats.supplier_id
ORDER BY id DESC, rating ASC
