SELECT e.full_name, e.manager_id, e.dept_id, d.dept_name, d.budget FROM employees e JOIN departments d ON e.dept_id = d.id
