SELECT id, job_title, hire_date
FROM (
    SELECT
        id,
        job_title,
        hire_date,
        ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY hire_date, id) AS row_num
    FROM employees
    WHERE YEAR(hire_date) >= 2025
) AS t
WHERE row_num <= 5;
