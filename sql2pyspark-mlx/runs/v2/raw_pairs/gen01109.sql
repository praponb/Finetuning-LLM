SELECT e.dept_id, e.salary, e.manager_id, d.budget, d.location FROM employees e JOIN departments d ON e.dept_id = d.id WHERE e.manager_id IS NOT NULL ORDER BY 3
