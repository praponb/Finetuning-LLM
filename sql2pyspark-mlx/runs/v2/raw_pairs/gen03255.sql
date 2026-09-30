SELECT
    e.id,
    e.hire_date AS employee_hire_date,
    m.is_remote AS manager_is_remote,
    m.manager_id AS manager_manager_id,
    m.email AS manager_email
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
WHERE m.email IS NOT NULL
ORDER BY m.email, e.id
LIMIT 5;
