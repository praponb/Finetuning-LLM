SELECT
    e.id,
    e.manager_id AS employee_manager_id,
    e.hire_date AS employee_hire_date,
    e.level AS employee_level,
    m.full_name AS manager_full_name,
    CAST(e.is_remote AS INT) AS is_remote_flag
FROM employees e
LEFT OUTER JOIN employees m ON e.manager_id = m.id
