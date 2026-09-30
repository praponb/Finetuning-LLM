SELECT
    id,
    dept_name,
    budget,
    location,
    DENSE_RANK() OVER (PARTITION BY dept_name ORDER BY budget DESC) AS dense_rnk,
    ROUND(budget / SUM(budget) OVER (PARTITION BY location), 3) AS budget_share
FROM departments
WHERE dept_name = 'Engineering'
