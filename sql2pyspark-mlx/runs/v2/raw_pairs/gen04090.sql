SELECT
    e.id,
    e.manager_id AS employee_manager_id,
    e.is_remote AS employee_is_remote,
    e.level AS employee_level,
    m.is_remote AS manager_is_remote,
    m.email AS manager_email,
    m.job_title AS manager_job_title
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
WHERE m.job_title = 'Intern'
