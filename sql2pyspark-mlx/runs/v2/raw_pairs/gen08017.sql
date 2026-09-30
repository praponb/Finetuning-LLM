SELECT d.id, d.budget, d.location
FROM departments d
WHERE d.dept_name IS NULL AND NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND NOT e.is_remote)
