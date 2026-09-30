SELECT
    e.id,
    e.manager_id AS employee_manager_id,
    e.is_remote AS employee_is_remote,
    m.level AS manager_level,
    WEEKOFYEAR(e.hire_date) AS hire_date_weekofyear
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
WHERE NOT m.is_remote
ORDER BY manager_level, hire_date_weekofyear DESC, id
LIMIT 20;
