SELECT country, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values
FROM suppliers
GROUP BY country
ORDER BY 2 DESC, country
LIMIT 5
