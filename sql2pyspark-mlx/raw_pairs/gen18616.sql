SELECT
    d.id,
    d.dept_name,
    d.location,
    d.budget
FROM departments d
WHERE EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id)
ORDER BY 4, id
LIMIT 1;
