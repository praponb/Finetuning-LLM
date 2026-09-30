SELECT
    e.id,
    e.level,
    e.is_remote,
    d.location
FROM employees e
JOIN departments d ON e.dept_id = d.id
ORDER BY level
