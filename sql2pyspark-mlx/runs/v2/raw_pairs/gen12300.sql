SELECT
    id,
    location,
    budget,
    dept_name,
    AVG(budget) OVER (PARTITION BY location) AS location_avg_budget,
    ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name), 3) AS budget_share
FROM departments;
