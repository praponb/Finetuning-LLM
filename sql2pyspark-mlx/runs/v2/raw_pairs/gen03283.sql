SELECT
    id,
    level,
    is_remote,
    hire_date,
    CONCAT_WS(',', full_name, job_title) AS full_name_job_title_key
FROM employees
WHERE is_remote;
