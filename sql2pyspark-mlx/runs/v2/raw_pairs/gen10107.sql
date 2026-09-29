SELECT
    e.id,
    e.email AS employee_email,
    e.hire_date AS employee_hire_date,
    m.full_name AS manager_full_name
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
WHERE e.manager_id = 12;
