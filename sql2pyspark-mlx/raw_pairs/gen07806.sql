SELECT
    id,
    dept_name,
    location,
    budget,
    FIRST_VALUE(budget) IGNORE NULLS OVER (ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_budget,
    AVG(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS budget_avg_2
FROM departments
