WITH ranked AS (
    SELECT
        id,
        manager_id,
        salary,
        hire_date,
        ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY salary DESC, id) AS row_num
    FROM employees
)
SELECT *
FROM ranked
WHERE row_num <= 2
