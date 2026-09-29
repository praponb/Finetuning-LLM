WITH RankedEmployees AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY dept_id ORDER BY hire_date) AS rnk
    FROM employees
)
SELECT
    id,
    dept_id,
    hire_date,
    is_remote
FROM RankedEmployees
WHERE rnk <= 1
ORDER BY id DESC
