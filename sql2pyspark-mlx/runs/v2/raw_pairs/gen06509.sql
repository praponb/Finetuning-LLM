SELECT
    e.id,
    e.manager_id AS employee_manager_id,
    e.is_remote AS employee_is_remote,
    e.email AS employee_email,
    m.email AS manager_email,
    m.is_remote AS manager_is_remote
FROM employees e
LEFT OUTER JOIN employees m ON e.manager_id = m.id
WHERE e.salary BETWEEN 123000 AND 138000 AND m.job_title = 'Intern';
