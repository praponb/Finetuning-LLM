SELECT
    e.id,
    e.salary AS employee_salary,
    e.email AS employee_email,
    e.job_title AS employee_job_title,
    m.manager_id AS manager_manager_id,
    m.email AS manager_email,
    m.is_remote AS manager_is_remote
FROM employees e
LEFT OUTER JOIN employees m ON e.manager_id = m.id
WHERE e.level IS NULL AND m.is_remote = TRUE;
