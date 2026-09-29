SELECT e.full_name, e.manager_id, e.level, d.budget, d.dept_name, d.location FROM employees e RIGHT JOIN departments d ON e.dept_id = d.id WHERE e.email IS NOT NULL
