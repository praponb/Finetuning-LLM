SELECT
    id,
    hire_date,
    level,
    salary,
    UPPER(full_name) AS full_name_upper,
    CASE WHEN is_remote THEN 'Yes' ELSE 'No' END AS is_remote_label
FROM employees
WHERE (job_title IN ('Manager', 'Engineer') AND salary < 120000) OR dept_id = 36;
