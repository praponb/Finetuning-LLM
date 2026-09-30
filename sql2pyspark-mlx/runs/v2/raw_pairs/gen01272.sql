SELECT brand, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(brand))) AS brand_values FROM products GROUP BY brand;
