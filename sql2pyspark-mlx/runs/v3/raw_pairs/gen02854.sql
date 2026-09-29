SELECT e.manager_id, d.budget, d.location, d.dept_name FROM employees e RIGHT JOIN departments d ON d.id = e.dept_id ORDER BY d.dept_name DESC
