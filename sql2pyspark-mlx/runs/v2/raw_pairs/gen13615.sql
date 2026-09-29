SELECT
    d.location,
    e.dept_id,
    e.is_remote,
    e.email
FROM departments d
LEFT JOIN employees e ON d.id = e.dept_id
