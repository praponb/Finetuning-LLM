SELECT city, MAX(sqft) AS max_sqft, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(region))) AS region_values FROM stores GROUP BY city;
