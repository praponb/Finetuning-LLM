SELECT city, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values FROM customers WHERE credit_limit BETWEEN 11900 AND 15600 GROUP BY city
