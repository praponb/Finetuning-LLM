SELECT
    id,
    location,
    budget,
    dept_name,
    SUM(budget) OVER (PARTITION BY location) AS location_sum_budget,
    ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name), 3) AS budget_share
FROM departments
ORDER BY budget ASC
