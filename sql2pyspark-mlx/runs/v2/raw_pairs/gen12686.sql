SELECT
    e.is_remote AS employee_is_remote,
    e.full_name AS employee_full_name,
    m.is_remote AS manager_is_remote,
    m.job_title AS manager_job_title
FROM employees e
JOIN employees m ON e.manager_id = m.id
WHERE m.manager_id IS NULL
ORDER BY manager_job_title ASC;
