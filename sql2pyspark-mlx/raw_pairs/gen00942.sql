SELECT d.id, d.dept_name
FROM departments d
WHERE d.dept_name = 'Marketing' AND NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id)
ORDER BY id;
