SELECT
    id,
    location,
    dept_name,
    budget,
    PERCENT_RANK() OVER (PARTITION BY location, dept_name ORDER BY budget DESC) AS budget_pct_rank,
    ROUND(AVG(budget) OVER (PARTITION BY location ORDER BY budget, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS budget_moving_avg_4
FROM departments
WHERE location = 'Bangkok';
