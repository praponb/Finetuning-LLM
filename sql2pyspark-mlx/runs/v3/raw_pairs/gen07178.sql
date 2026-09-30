SELECT
    job_title,
    full_name,
    CAST(is_remote AS INT) AS is_remote_flag,
    LAST_DAY(hire_date) AS hire_date_month_end
FROM employees
WHERE manager_id IS NOT NULL;
