SELECT city, region, SORT_ARRAY(COLLECT_SET(city)) AS city_list
FROM stores
GROUP BY city, region
ORDER BY city;
