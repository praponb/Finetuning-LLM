SELECT is_active, MIN(age) AS min_age, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values FROM customers WHERE is_active GROUP BY is_active HAVING min_age < 59 ORDER BY city_values DESC;
