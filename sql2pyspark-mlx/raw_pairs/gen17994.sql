SELECT departments.location, MAX(employees.hire_date) AS last_hire_date, SUM(CASE WHEN employees.level = 'L2' THEN employees.salary ELSE 0 END) AS l2_salary
FROM employees
LEFT JOIN departments ON employees.dept_id = departments.id
GROUP BY departments.location
ORDER BY last_hire_date, location
LIMIT 10
