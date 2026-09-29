SELECT
    d.location,
    e.is_remote,
    e.dept_id,
    e.salary
FROM departments d
LEFT JOIN employees e ON d.id = e.dept_id
WHERE d.location IN ('Tokyo', 'New York')
ORDER BY salary
