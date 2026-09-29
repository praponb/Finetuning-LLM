SELECT e.email, e.manager_id, e.is_remote, d.location, d.dept_name, ROUND(d.budget * 1.15, 2) adjusted_budget FROM employees e JOIN departments d ON d.id = e.dept_id ORDER BY manager_id DESC;
