SELECT
    id,
    level,
    salary,
    DENSE_RANK() OVER (PARTITION BY level ORDER BY salary) AS dense_rnk,
    NTILE(4) OVER (PARTITION BY level ORDER BY salary DESC, id) AS bucket
FROM employees
WHERE manager_id IN (2, 12, 15, 34)
