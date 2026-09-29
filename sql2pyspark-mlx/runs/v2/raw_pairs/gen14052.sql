SELECT city, SUM(CASE WHEN city = 'Tokyo' THEN 1 ELSE 0 END) AS tokyo_count, SORT_ARRAY(COLLECT_SET(city)) AS city_list
FROM stores
GROUP BY city
ORDER BY tokyo_count DESC, city
LIMIT 50;
