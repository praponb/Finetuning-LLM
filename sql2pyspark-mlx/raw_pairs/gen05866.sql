SELECT d.id, d.budget FROM departments d WHERE d.budget BETWEEN 1601000 AND 3890000 AND NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.level = 'L4');
