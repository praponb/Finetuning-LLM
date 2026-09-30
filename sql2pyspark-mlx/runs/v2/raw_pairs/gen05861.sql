SELECT CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values FROM suppliers ORDER BY city_values
