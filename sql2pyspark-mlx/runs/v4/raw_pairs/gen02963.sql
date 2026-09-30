SELECT city, ROUND(STDDEV(sqft), 2) AS stddev_sqft, SORT_ARRAY(COLLECT_SET(region)) AS region_list
FROM stores
GROUP BY 1
ORDER BY city DESC
LIMIT 20;
