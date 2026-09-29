SELECT d.id, d.budget
FROM departments d
WHERE d.location = 'Tokyo' AND EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id)
ORDER BY d.budget DESC;
