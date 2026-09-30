SELECT d.id, d.budget, d.location
FROM departments d
WHERE d.budget >= 3489000 AND EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id)
ORDER BY id
