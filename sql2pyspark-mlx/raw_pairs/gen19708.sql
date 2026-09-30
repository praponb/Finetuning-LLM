WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary, id) AS row_num
    FROM employees
)
SELECT *
FROM numbered
WHERE row_num <= 5
ORDER BY dept_id
