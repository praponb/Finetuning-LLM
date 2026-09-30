SELECT
    e.id,
    e.level AS employee_level,
    m.is_remote AS manager_is_remote,
    m.email AS manager_email
FROM employees AS e
LEFT JOIN employees AS m ON e.manager_id = m.id
WHERE m.salary >= 100000
