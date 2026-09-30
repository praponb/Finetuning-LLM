SELECT e.id, e.dept_id, d.location, d.budget, d.dept_name FROM employees e INNER JOIN departments d ON e.dept_id = d.id WHERE YEAR(e.hire_date) = 2023 ORDER BY dept_id
