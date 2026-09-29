SELECT ROUND((100 * SUM(CASE WHEN channel = 'web' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_web, SORT_ARRAY(COLLECT_SET(status)) AS status_list
FROM orders
ORDER BY pct_web DESC;
