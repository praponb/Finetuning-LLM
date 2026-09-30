SELECT d.id, d.location, d.dept_name
FROM departments d
WHERE d.location <> 'New York' AND EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id)
