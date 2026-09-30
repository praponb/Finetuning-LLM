SELECT
    id,
    location,
    budget,
    ROUND(AVG(budget) OVER (PARTITION BY location ORDER BY budget, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS budget_moving_avg_4
FROM departments
WHERE budget BETWEEN 1382000 AND 3901000
