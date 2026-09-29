SELECT d.id, d.dept_name FROM departments d WHERE EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.dept_id IN (3, 14, 20, 29))
