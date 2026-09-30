SELECT
    d.id,
    d.location,
    d.budget,
    d.dept_name
FROM departments d
WHERE d.location IS NOT NULL AND NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.hire_date BETWEEN '2023-08-01' AND '2025-05-23')
