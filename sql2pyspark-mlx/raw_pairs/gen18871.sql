SELECT d.location, MIN(e.hire_date) AS first_hire_date, COUNT(*) AS num_employees
FROM employees e
LEFT JOIN departments d ON d.id = e.dept_id
GROUP BY d.location
