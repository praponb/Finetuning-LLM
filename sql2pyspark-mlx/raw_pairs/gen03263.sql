SELECT d.dept_name, d.budget, d.location, e.manager_id FROM departments d LEFT JOIN employees e ON d.id = e.dept_id WHERE e.salary > 116000 ORDER BY d.dept_name;
