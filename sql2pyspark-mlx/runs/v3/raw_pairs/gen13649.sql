SELECT d.location, SORT_ARRAY(COLLECT_SET(e.level)) AS level_list
FROM employees e
RIGHT JOIN departments d ON e.dept_id = d.id
WHERE e.full_name LIKE 'Mi%'
GROUP BY d.location
ORDER BY location DESC;
