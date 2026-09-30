SELECT d.id, d.budget, d.location
FROM departments d
WHERE EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.is_remote = TRUE)
ORDER BY d.budget;
