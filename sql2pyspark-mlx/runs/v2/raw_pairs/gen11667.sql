WITH numbered AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY job_title ORDER BY hire_date DESC) AS rn
    FROM employees
)
SELECT *
FROM numbered
WHERE rn <= 3
