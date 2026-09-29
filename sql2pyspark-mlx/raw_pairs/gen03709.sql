SELECT
    id,
    dept_id,
    hire_date,
    salary,
    CONCAT_WS(',', level, email) AS level_email_key,
    CONCAT_WS(',', full_name, job_title) AS full_name_job_title_key
FROM employees
WHERE is_remote = TRUE
