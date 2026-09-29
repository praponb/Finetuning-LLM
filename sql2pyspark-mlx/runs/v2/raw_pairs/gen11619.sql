SELECT d.id, d.dept_name FROM departments d WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.dept_id IN (10, 13, 26, 35)) ORDER BY id DESC
