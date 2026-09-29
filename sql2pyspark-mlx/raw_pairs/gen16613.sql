SELECT d.budget, e.full_name FROM departments d LEFT JOIN employees e ON e.dept_id = d.id WHERE e.manager_id IS NOT NULL
