SELECT
    id,
    level,
    salary,
    RANK() OVER (PARTITION BY level ORDER BY salary) AS rnk
FROM employees
WHERE manager_id IS NULL
