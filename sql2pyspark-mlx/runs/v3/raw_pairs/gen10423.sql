SELECT
    e.id,
    e.manager_id AS employee_manager_id,
    e.salary AS employee_salary,
    m.email AS manager_email
FROM employees e
LEFT OUTER JOIN employees m ON e.manager_id = m.id
WHERE m.email LIKE '%@gmail.com'
ORDER BY e.manager_id DESC
