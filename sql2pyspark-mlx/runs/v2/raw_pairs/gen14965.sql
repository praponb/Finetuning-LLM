SELECT d.dept_name, MIN(e.hire_date) AS first_hire_date
FROM employees e
INNER JOIN departments d ON e.dept_id = d.id
GROUP BY d.dept_name
ORDER BY dept_name DESC
