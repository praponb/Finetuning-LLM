SELECT d.location, d.budget, e.level FROM departments d LEFT JOIN employees e ON d.id = e.dept_id ORDER BY d.location ASC
