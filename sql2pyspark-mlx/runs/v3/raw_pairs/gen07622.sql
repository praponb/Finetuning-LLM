SELECT
    id,
    dept_name,
    budget,
    ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name), 3) AS budget_share,
    RANK() OVER (PARTITION BY dept_name ORDER BY budget) AS budget_rank
FROM departments;
