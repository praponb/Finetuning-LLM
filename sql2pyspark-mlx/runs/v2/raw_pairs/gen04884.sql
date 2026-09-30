SELECT CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(category))) AS category_values FROM products ORDER BY category_values
