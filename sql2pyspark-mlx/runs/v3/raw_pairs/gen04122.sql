SELECT
    d.id,
    d.location,
    d.dept_name,
    d.budget
FROM departments d
WHERE d.location IN ('London', 'Berlin', 'Chiang Mai') AND NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id)
