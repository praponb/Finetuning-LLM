SELECT
    id,
    location,
    budget,
    LAG(budget, 1) OVER (PARTITION BY location ORDER BY budget, id) AS prev_budget,
    ROUND(AVG(budget) OVER (ORDER BY budget, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS budget_moving_avg_3
FROM departments
