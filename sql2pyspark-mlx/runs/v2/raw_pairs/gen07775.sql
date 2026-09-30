SELECT e.id, e.salary, d.dept_name, d.budget, d.location FROM employees e LEFT JOIN departments d ON e.dept_id = d.id ORDER BY id DESC LIMIT 1
