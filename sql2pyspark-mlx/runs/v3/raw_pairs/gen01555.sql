SELECT departments.id, COUNT(employees.id) AS employee_count, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(employees.level))) AS level_values
FROM departments
LEFT OUTER JOIN employees ON departments.id = employees.dept_id
WHERE MONTH(employees.hire_date) IN (3, 5, 10)
GROUP BY departments.id
HAVING COUNT(employees.id) > 3;
