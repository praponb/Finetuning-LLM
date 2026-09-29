SELECT CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values, ROUND((100 * SUM(CASE WHEN level = 'L1' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_l1
FROM employees
WHERE dept_id IN (3, 17, 25, 37)
ORDER BY pct_l1 DESC
