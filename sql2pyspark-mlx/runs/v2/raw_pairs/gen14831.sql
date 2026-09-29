SELECT country, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values, COUNT(CASE WHEN city = 'Tokyo' THEN 1 END) AS tokyo_count FROM customers GROUP BY country
