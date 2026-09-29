SELECT CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values, COUNT(DISTINCT credit_limit) AS unique_countrys, MIN(age) AS min_age FROM customers WHERE UPPER(TRIM(name)) = 'PIM CHAIYAPORN'
