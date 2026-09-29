SELECT city, country, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values, SORT_ARRAY(COLLECT_SET(country)) AS country_list, MAX(rating) AS max_rating FROM suppliers GROUP BY city, country;
