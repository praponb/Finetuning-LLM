SELECT
    id,
    location,
    budget,
    SUM(budget) OVER (PARTITION BY location) AS location_sum_budget
FROM departments
ORDER BY budget DESC;
