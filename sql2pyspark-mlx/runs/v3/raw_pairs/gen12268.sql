SELECT
    manager_id,
    email,
    is_remote,
    UPPER(job_title) AS job_title_upper
FROM employees
WHERE is_remote = TRUE AND email LIKE '%@outlook.com'
