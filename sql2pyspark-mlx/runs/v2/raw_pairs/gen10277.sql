SELECT city, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(region))) AS region_values, ROUND((100 * SUM(CASE WHEN city = 'Tokyo' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_tokyo
FROM stores
WHERE region <> 'North' AND city = 'New York'
GROUP BY city
ORDER BY pct_tokyo ASC, city ASC
LIMIT 10;
