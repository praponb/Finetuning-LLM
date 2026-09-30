SELECT d.id, d.location, d.budget FROM departments d WHERE d.budget > 1544000 AND NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id) ORDER BY d.location;
