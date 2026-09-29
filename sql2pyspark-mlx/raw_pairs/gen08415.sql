SELECT
    e.is_remote AS employee_is_remote,
    e.full_name AS employee_full_name,
    m.is_remote AS manager_is_remote,
    m.manager_id AS manager_manager_id,
    m.full_name AS manager_full_name
FROM employees e
JOIN employees m ON e.manager_id = m.id
WHERE m.dept_id = 10;
