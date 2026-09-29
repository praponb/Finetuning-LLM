SELECT
    e.id,
    e.is_remote AS employee_is_remote,
    m.full_name AS manager_full_name,
    m.level AS manager_level
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
WHERE m.full_name LIKE 'Car%'
