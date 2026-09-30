SELECT
    e.email AS employee_email,
    e.dept_id AS employee_dept_id,
    m.email AS manager_email,
    m.dept_id AS manager_dept_id
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
WHERE e.dept_id = 17
ORDER BY employee_email, manager_email DESC
