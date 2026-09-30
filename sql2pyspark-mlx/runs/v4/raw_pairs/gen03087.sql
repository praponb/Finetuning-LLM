SELECT city, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) AS country_values, SORT_ARRAY(COLLECT_SET(city)) AS city_list
FROM suppliers
WHERE city <> 'Tokyo'
GROUP BY city
ORDER BY city
