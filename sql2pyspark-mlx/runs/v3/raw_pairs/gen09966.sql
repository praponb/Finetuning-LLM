SELECT
    id,
    job_title,
    hire_date,
    RANK() OVER (PARTITION BY job_title ORDER BY hire_date) AS rnk
FROM employees
WHERE dept_id = 25;
