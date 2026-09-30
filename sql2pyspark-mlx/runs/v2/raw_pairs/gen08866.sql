SELECT brand, SORT_ARRAY(COLLECT_SET(category)) AS category_list, MAX(price) AS max_price FROM products GROUP BY brand HAVING MAX(price) > 560 ORDER BY 3 DESC
