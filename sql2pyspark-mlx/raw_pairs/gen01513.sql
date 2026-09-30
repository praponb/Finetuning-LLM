WITH TopEmployees AS (
    SELECT *, RANK() OVER (PARTITION BY job_title ORDER BY salary) AS rn
    FROM employees
)
SELECT *
FROM TopEmployees
WHERE rn <= 3
