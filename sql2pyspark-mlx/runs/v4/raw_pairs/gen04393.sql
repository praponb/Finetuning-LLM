SELECT
    e.id,
    e.manager_id,
    e.full_name,
    d.dept_name,
    d.location
FROM employees e
LEFT JOIN departments d ON e.dept_id = d.id
WHERE d.location = 'Tokyo';
