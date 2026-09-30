SELECT
    e.id,
    e.manager_id employee_manager_id,
    e.level employee_level,
    m.manager_id manager_manager_id,
    m.is_remote manager_is_remote,
    m.level manager_level
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
WHERE m.hire_date < '2025-02-04' AND LENGTH(m.email) > 8
