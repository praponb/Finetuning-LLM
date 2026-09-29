SELECT d.id, d.dept_name
FROM departments d
WHERE d.dept_name IN ('Marketing', 'HR') AND EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.dept_id IN (3, 24, 33))
ORDER BY id DESC
LIMIT 3;
