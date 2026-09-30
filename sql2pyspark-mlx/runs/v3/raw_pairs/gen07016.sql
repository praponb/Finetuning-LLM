SELECT
    level,
    full_name,
    job_title,
    CONCAT(full_name, ' - ', job_title) AS full_name_job_title
FROM employees
WHERE is_remote AND email LIKE '%@outlook.com';
