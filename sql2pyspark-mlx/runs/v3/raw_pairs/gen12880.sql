SELECT
    e.is_remote AS employee_is_remote,
    e.manager_id AS employee_manager_id,
    m.salary AS manager_salary,
    m.email AS manager_email,
    m.level AS manager_level
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
WHERE NOT e.is_remote AND m.full_name LIKE 'A%';
