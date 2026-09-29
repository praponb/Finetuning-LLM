SELECT
    e.id,
    e.salary AS employee_salary,
    e.full_name AS employee_full_name,
    m.email AS manager_email,
    m.full_name AS manager_full_name,
    LENGTH(e.email) AS email_length
FROM employees AS e
LEFT JOIN employees AS m ON e.manager_id = m.id
WHERE YEAR(m.hire_date) >= 2025 AND e.full_name LIKE 'Noa%'
