SELECT
    e.email AS employee_email,
    m.is_remote AS manager_is_remote,
    m.job_title AS manager_job_title,
    m.level AS manager_level,
    LOWER(m.email) AS email_lower
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
