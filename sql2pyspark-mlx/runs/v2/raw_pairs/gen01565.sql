SELECT category, YEAR(launch_date) AS launch_year, brand, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(brand))) AS brand_values FROM products GROUP BY category, YEAR(launch_date), brand
