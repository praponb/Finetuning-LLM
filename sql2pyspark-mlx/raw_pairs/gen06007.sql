SELECT
    e.id,
    e.job_title,
    d.location,
    d.dept_name
FROM employees e
LEFT JOIN departments d ON e.dept_id = d.id
WHERE NOT (d.location IS NULL AND d.location IN ('Bangkok', 'New York', 'Berlin'));
