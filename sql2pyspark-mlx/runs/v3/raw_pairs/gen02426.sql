SELECT d.location, AVG(e.salary) AS avg_salary, MAX(e.hire_date) AS last_hire_date FROM employees e LEFT JOIN departments d ON e.dept_id = d.id GROUP BY d.location ORDER BY location
