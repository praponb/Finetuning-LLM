SELECT
    d.budget,
    d.location,
    d.dept_name,
    e.hire_date
FROM departments d
LEFT JOIN employees e ON d.id = e.dept_id
WHERE d.location IN ('London', 'Chiang Mai', 'Tokyo')
