WITH TopEmployees AS (
    SELECT
        id,
        level,
        salary,
        job_title,
        RANK() OVER (PARTITION BY level ORDER BY salary) AS rnk
    FROM employees
    WHERE dept_id IN (1, 13)
)
SELECT *
FROM TopEmployees
WHERE rnk <= 3
ORDER BY job_title
