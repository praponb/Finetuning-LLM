SELECT d.id, d.location
FROM departments d
WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.is_remote)
ORDER BY d.id ASC
LIMIT 10;
