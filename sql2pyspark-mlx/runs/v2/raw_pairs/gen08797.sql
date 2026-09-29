SELECT
    e.full_name AS employee_full_name,
    e.is_remote AS employee_is_remote,
    m.dept_id AS manager_dept_id,
    m.email AS manager_email
FROM employees e
INNER JOIN employees m ON e.manager_id = m.id
WHERE m.manager_id IS NOT NULL
ORDER BY employee_full_name DESC, m.email DESC;
