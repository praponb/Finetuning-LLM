SELECT
    d.id,
    d.location,
    d.budget,
    d.dept_name
FROM departments d
WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.level IN ('L3', 'L2'))
ORDER BY id DESC
