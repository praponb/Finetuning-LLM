WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary DESC, id) AS row_num
    FROM employees
)
SELECT id, level, salary
FROM numbered
WHERE row_num <= 3
