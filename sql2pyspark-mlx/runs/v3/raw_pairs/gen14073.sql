SELECT e.id, e.level, d.dept_name, d.budget, d.location FROM employees e RIGHT JOIN departments d ON e.dept_id = d.id
