SELECT city, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values, SUM(CASE WHEN city = 'Tokyo' THEN 1 ELSE 0 END) AS tokyo_count
FROM suppliers
GROUP BY city
ORDER BY tokyo_count DESC, city
LIMIT 5;
