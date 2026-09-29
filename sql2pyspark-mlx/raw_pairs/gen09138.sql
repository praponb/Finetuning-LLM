SELECT d.id, d.location, d.budget, d.dept_name FROM departments d WHERE EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND LENGTH(e.full_name) < 7)
