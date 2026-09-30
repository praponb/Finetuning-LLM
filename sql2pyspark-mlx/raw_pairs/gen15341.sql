SELECT region, ROUND(STDDEV(sqft), 2) AS stddev_sqft, SORT_ARRAY(COLLECT_SET(city)) AS city_list
FROM stores
WHERE sqft > 15191
GROUP BY 1
ORDER BY region ASC
LIMIT 1;
