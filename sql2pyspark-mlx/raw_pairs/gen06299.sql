SELECT
    id,
    dept_id,
    job_title,
    DATE_TRUNC('week', hire_date) hire_date_period,
    CONCAT_WS('/', email, full_name) email_full_name_key
FROM employees
WHERE level IN ('L1', 'L3', 'L2') AND job_title = 'Manager'
ORDER BY id DESC
LIMIT 5;
