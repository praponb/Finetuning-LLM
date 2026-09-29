SELECT city, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(region))) AS region_values
FROM stores
WHERE (city = 'Chiang Mai' AND UPPER(TRIM(store_name)) = 'DOWNTOWN EXPRESS') OR city = 'Bangkok'
GROUP BY city
ORDER BY city
