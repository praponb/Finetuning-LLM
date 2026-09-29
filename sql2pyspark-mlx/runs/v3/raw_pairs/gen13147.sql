SELECT
    e.id,
    e.is_remote AS employee_is_remote,
    e.full_name AS employee_full_name,
    m.manager_id AS manager_manager_id,
    TRIM(e.level) AS clean_level
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
WHERE e.level IN ('L2', 'L4');
