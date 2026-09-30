SELECT
    s.id,
    s.city,
    t.category_list,
    t.brand_values
FROM suppliers s
INNER JOIN (SELECT supplier_id, SORT_ARRAY(COLLECT_SET(category)) AS category_list, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(brand))) AS brand_values
    FROM products
    GROUP BY supplier_id) t ON s.id = t.supplier_id
