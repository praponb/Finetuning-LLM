SELECT
    id,
    dept_name,
    budget,
    location,
    ROUND(AVG(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS budget_moving_avg_7,
    SUM(budget) OVER (PARTITION BY location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_budget
FROM departments
WHERE location IN ('New York', 'Chiang Mai', 'London')
ORDER BY budget_moving_avg_7;
