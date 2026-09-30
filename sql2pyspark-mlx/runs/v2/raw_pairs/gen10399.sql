SELECT
    id,
    job_title,
    hire_date,
    ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY hire_date, id) AS hire_date_row
FROM employees
WHERE salary < 130000
