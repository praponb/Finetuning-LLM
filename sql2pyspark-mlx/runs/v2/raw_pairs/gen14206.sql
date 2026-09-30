SELECT
    e.id,
    e.dept_id AS employee_dept_id,
    e.email AS employee_email,
    e.salary AS employee_salary,
    m.email AS manager_email,
    m.full_name AS manager_full_name
FROM employees e
JOIN employees m ON e.manager_id = m.id
ORDER BY manager_email ASC
