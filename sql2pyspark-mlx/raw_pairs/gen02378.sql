SELECT d.id, d.dept_name, d.location FROM departments d WHERE EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.dept_id IN (33, 40))
