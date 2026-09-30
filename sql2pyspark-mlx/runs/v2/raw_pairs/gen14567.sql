SELECT
    e.id,
    e.manager_id AS employee_manager_id,
    e.is_remote AS employee_is_remote,
    m.job_title AS manager_job_title,
    UPPER(TRIM(e.full_name)) AS full_name_normalized
FROM employees e
INNER JOIN employees m ON e.manager_id = m.id
WHERE e.is_remote
