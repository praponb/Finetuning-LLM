SELECT
    d.id,
    d.location,
    d.dept_name,
    d.budget
FROM departments d
WHERE d.budget < 1421000 AND NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id)
ORDER BY dept_name DESC
