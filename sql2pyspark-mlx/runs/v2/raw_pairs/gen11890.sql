SELECT
    e.id,
    e.hire_date AS employee_hire_date,
    e.salary AS employee_salary,
    e.email AS employee_email,
    m.email AS manager_email,
    m.level AS manager_level
FROM employees e
LEFT OUTER JOIN employees m ON e.manager_id = m.id
