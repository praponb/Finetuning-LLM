SELECT
    e.full_name AS employee_full_name,
    e.is_remote AS employee_is_remote,
    e.hire_date AS employee_hire_date,
    m.is_remote AS manager_is_remote
FROM employees AS e
JOIN employees AS m ON e.manager_id = m.id
