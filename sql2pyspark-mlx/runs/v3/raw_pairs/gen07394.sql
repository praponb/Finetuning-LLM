SELECT category, SORT_ARRAY(COLLECT_SET(category)) AS category_list FROM products GROUP BY category
