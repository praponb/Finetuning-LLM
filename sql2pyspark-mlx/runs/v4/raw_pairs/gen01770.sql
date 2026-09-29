SELECT
    e.id,
    e.hire_date AS employee_hire_date,
    m.dept_id AS manager_dept_id,
    m.is_remote AS manager_is_remote,
    m.email AS manager_email
FROM employees e
LEFT OUTER JOIN employees m ON e.manager_id = m.id
WHERE e.level IN ('L3', 'L4', 'L5')
