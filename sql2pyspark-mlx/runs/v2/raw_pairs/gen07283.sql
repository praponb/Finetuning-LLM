SELECT
    e.is_remote AS employee_is_remote,
    e.job_title AS employee_job_title,
    e.email AS employee_email,
    m.manager_id AS manager_manager_id,
    m.email AS manager_email,
    m.full_name AS manager_full_name
FROM employees e
JOIN employees m ON e.manager_id = m.id;
