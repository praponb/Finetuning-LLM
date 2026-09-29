WITH numbered AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY level ORDER BY salary DESC) rn
    FROM employees
)
SELECT
    id,
    level,
    salary,
    manager_id
FROM numbered
WHERE rn <= 3;
