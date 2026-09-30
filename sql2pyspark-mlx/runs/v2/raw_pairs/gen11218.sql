SELECT country, city, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values FROM suppliers WHERE city = 'New York' OR city = 'New York' GROUP BY CUBE(country, city);
