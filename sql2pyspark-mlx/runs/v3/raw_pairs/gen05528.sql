SELECT
    id,
    job_title,
    hire_date,
    is_remote,
    UPPER(level) AS level_upper,
    REGEXP_EXTRACT(email, '@(.+)$', 1) AS email_domain
FROM employees
WHERE is_remote = TRUE AND full_name LIKE 'No%';
