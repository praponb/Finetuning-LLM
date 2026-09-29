SELECT d.id, d.location FROM departments d WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.dept_id IN (3, 30));
