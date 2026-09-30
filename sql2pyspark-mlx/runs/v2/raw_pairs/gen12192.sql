SELECT
    id,
    hire_date,
    job_title,
    LAST_VALUE(job_title) OVER (ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_job_title
FROM employees;
