WITH Ranked AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY manager_id ORDER BY hire_date DESC) AS row_num
    FROM employees
)
SELECT *
FROM Ranked
WHERE row_num <= 5;
