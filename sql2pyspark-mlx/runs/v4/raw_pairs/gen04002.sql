SELECT city, SORT_ARRAY(COLLECT_SET(city)) AS city_list, COUNT(*) AS total_count
FROM stores
GROUP BY city
ORDER BY total_count DESC, city
LIMIT 3;
