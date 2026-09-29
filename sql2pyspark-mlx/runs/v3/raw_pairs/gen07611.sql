SELECT
    id,
    job_title,
    hire_date,
    dept_id
FROM (
    SELECT
        id,
        job_title,
        hire_date,
        dept_id,
        DENSE_RANK() OVER (PARTITION BY job_title ORDER BY hire_date DESC) AS row_num
    FROM employees
) AS r
WHERE row_num <= 1
ORDER BY job_title
