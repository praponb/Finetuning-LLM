SELECT d.location, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(e.level))) AS level_values, MIN(e.hire_date) AS first_hire_date
FROM employees e
LEFT JOIN departments d ON e.dept_id = d.id
WHERE e.dept_id IN (9, 10, 12, 24)
GROUP BY d.location
