SELECT
    s.id,
    s.country,
    s.rating,
    t.product_count,
    t.category_list
FROM suppliers s
LEFT JOIN (SELECT supplier_id, COUNT(*) AS product_count, SORT_ARRAY(COLLECT_SET(category)) AS category_list
    FROM products
    WHERE product_name NOT LIKE '%Running%'
    GROUP BY supplier_id) t ON s.id = t.supplier_id
