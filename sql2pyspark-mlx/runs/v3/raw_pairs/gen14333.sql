SELECT d.id, d.budget
FROM departments d
WHERE EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.dept_id IN (8, 13, 27))
ORDER BY d.id DESC;
