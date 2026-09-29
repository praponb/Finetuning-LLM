SELECT
    id,
    dept_id,
    hire_date,
    job_title,
    FIRST_VALUE(job_title) OVER (PARTITION BY dept_id ORDER BY hire_date DESC, id) AS first_job_title
FROM employees
ORDER BY hire_date DESC, 5 DESC
