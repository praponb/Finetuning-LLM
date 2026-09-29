SELECT
    hire_date,
    is_remote,
    salary,
    REPLACE(full_name, ' ', '_') AS full_name_slug
FROM employees
WHERE job_title IN ('Intern', 'Analyst')
