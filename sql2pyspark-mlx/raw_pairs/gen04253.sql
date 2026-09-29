SELECT d.id, d.budget FROM departments d WHERE d.budget BETWEEN 3164000 AND 3492000 AND EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.dept_id IN (2, 32))
