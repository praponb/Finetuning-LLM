SELECT
    e.id,
    e.email AS employee_email,
    e.salary AS employee_salary,
    m.full_name AS manager_full_name
FROM employees AS e
LEFT JOIN employees AS m ON e.manager_id = m.id
ORDER BY e.salary DESC, id
LIMIT 50;
