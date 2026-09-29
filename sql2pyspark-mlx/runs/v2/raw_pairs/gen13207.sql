SELECT departments.dept_name, SORT_ARRAY(COLLECT_SET(employees.level)) AS level_list
FROM employees
FULL OUTER JOIN departments ON employees.dept_id = departments.id
GROUP BY departments.dept_name
ORDER BY dept_name DESC;
