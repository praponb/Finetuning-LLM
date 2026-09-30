WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY hire_date, id) AS row_num
    FROM employees
    WHERE manager_id IN (1, 17, 32, 33)
)
SELECT *
FROM numbered
WHERE row_num <= 3;
