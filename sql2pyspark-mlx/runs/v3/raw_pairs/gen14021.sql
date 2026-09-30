SELECT city, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) AS country_values FROM suppliers GROUP BY city ORDER BY country_values;
