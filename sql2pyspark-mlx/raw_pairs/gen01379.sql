SELECT
    id,
    level,
    salary,
    DENSE_RANK() OVER (PARTITION BY level ORDER BY salary) AS salary_dense_rank
FROM employees
WHERE full_name LIKE '%Kanya%'
