SELECT d.id, d.budget, d.location, d.dept_name FROM departments d WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.dept_id IN (19, 30, 34, 36))
