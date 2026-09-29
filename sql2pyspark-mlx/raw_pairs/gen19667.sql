SELECT level, ROUND(STDDEV(salary), 2) AS stddev_salary, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values
FROM employees
WHERE level = 'L2'
GROUP BY level
ORDER BY level_values DESC;
