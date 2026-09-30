SELECT
    e.id,
    e.manager_id AS employee_manager_id,
    m.hire_date AS manager_hire_date,
    m.full_name AS manager_full_name
FROM employees e
LEFT OUTER JOIN employees m ON e.manager_id = m.id;
