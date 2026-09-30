SELECT
    id,
    location,
    budget,
    dept_name,
    MAX(budget) OVER (PARTITION BY location) AS location_max_budget,
    ROUND(budget / SUM(budget) OVER (PARTITION BY location, dept_name), 3) AS budget_share
FROM departments;
