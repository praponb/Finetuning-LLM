SELECT d.id, d.location FROM departments d WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.salary BETWEEN 58000 AND 78000) ORDER BY id DESC, location
