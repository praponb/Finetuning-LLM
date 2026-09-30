SELECT category, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(category))) AS category_values
FROM products
GROUP BY category
