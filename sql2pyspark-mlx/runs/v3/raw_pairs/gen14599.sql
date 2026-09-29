SELECT category, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(category))) AS category_values, MAX(price) AS max_price
FROM products
GROUP BY category
ORDER BY category, max_price
