SELECT e.id, e.dept_id, e.hire_date, d.budget, d.location FROM employees e JOIN departments d ON e.dept_id = d.id WHERE e.manager_id IN (13, 23, 33, 36) AND e.is_remote
