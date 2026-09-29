WITH numbered AS (
    SELECT
        id,
        job_title,
        hire_date,
        level,
        ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY hire_date DESC, id) AS rn
    FROM employees
)
SELECT
    id,
    job_title,
    hire_date,
    level
FROM numbered
WHERE rn = 1
