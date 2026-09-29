SELECT d.budget, e.email, e.level FROM departments d INNER JOIN employees e ON e.dept_id = d.id ORDER BY level DESC;
