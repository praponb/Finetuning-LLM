SELECT
    d.budget,
    d.location,
    e.salary,
    e.level,
    e.dept_id,
    UPPER(e.full_name) AS full_name_upper
FROM departments d
LEFT JOIN employees e ON e.dept_id = d.id
WHERE d.location IN ('London', 'Bangkok', 'Chiang Mai')
ORDER BY e.salary
