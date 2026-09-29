SELECT
    id,
    dept_name,
    budget,
    RANK() OVER (PARTITION BY dept_name ORDER BY budget DESC) rnk
FROM departments
WHERE budget < 1742000
ORDER BY id DESC
