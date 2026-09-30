SELECT
    id,
    job_title,
    full_name,
    DATEDIFF(CURRENT_DATE(), hire_date) AS days_since_hire_date,
    IF(is_remote, 'Yes', 'No') AS is_remote_label
FROM employees
WHERE is_remote = TRUE
ORDER BY is_remote_label, id
LIMIT 20;
