SELECT d.id, d.location, d.budget
FROM departments d
WHERE EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.full_name IS NOT NULL)
