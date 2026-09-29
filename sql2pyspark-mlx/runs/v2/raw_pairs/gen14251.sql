SELECT supplier_id, SORT_ARRAY(COLLECT_SET(category)) AS category_list
FROM products
GROUP BY supplier_id
ORDER BY supplier_id
LIMIT 20
