SELECT ROUND(AVG(price), 2) AS avg_price, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(brand))) AS brand_values FROM products WHERE (cost >= 670 AND cost >= 300) OR price > 490
