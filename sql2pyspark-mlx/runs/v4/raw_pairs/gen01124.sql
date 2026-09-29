SELECT
    id,
    location,
    budget,
    SUM(budget) OVER (PARTITION BY location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_budget
FROM departments;
