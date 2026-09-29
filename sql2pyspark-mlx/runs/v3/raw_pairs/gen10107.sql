SELECT
    e.id,
    e.level AS employee_level,
    e.dept_id AS employee_dept_id,
    e.is_remote AS employee_is_remote,
    m.salary AS manager_salary,
    IF(m.is_remote, 'Yes', 'No') AS is_remote_label
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
WHERE e.dept_id IN (16, 21, 30, 34);
