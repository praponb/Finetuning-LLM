SELECT
    e.id,
    e.level AS employee_level,
    e.email AS employee_email,
    m.is_remote AS manager_is_remote,
    ROUND(m.salary * 1.1, 2) AS salary_scaled
FROM employees e
LEFT OUTER JOIN employees m ON e.manager_id = m.id
