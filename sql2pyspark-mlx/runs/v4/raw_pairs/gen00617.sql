SELECT CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(brand))) AS brand_values, MIN(price) AS min_price FROM products
