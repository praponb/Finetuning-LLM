SELECT
    e.id,
    e.email AS employee_email,
    m.dept_id AS manager_dept_id,
    m.is_remote AS manager_is_remote,
    m.salary AS manager_salary
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
