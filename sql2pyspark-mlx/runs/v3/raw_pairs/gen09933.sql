SELECT d.id, d.location
FROM departments d
WHERE d.location NOT IN ('Tokyo', 'London') AND EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id)
