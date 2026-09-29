SELECT d.id, d.dept_name, COUNT(e.id) AS employee_count, MAX(e.salary) - MIN(e.salary) AS salary_range FROM departments d LEFT JOIN employees e ON e.dept_id = d.id GROUP BY d.id, d.dept_name
