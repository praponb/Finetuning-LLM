SELECT
    id,
    dept_name,
    budget,
    DENSE_RANK() OVER (PARTITION BY dept_name ORDER BY budget DESC) AS dense_rnk
FROM departments
WHERE location IS NULL
