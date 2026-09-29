SELECT id, budget, ROUND(AVG(budget) OVER (ORDER BY budget, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS budget_moving_avg_7
FROM departments
