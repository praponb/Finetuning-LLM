SELECT d.id, d.location
FROM departments d
WHERE d.budget < 1108000 AND EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id);
