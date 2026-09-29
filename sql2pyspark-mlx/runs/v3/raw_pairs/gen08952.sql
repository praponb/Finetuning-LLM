SELECT
    id,
    full_name,
    manager_id,
    job_title,
    REGEXP_EXTRACT(email, '@(.+)$', 1) AS email_domain,
    CONCAT(full_name, ' - ', job_title) AS full_name_job_title
FROM employees
WHERE NOT is_remote AND dept_id IN (6, 16, 21, 37) AND level IN ('L5', 'L1', 'L2')
ORDER BY id DESC
