SELECT city, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values, ROUND((100 * SUM(CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_bangkok
FROM stores
WHERE store_name IS NOT NULL AND sqft != 11356
GROUP BY city
ORDER BY city
