SELECT city, SORT_ARRAY(COLLECT_SET(city)) AS city_list
FROM stores
WHERE region IN ('West', 'Central', 'South')
GROUP BY city
ORDER BY city;
