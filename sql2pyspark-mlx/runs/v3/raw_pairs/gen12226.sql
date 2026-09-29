SELECT
    e.id,
    e.salary AS employee_salary,
    m.email AS manager_email,
    m.salary AS manager_salary,
    m.level AS manager_level
FROM employees e
INNER JOIN employees m ON e.manager_id = m.id;
