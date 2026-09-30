SELECT
    id,
    budget,
    location,
    LAG(budget, 1) OVER (ORDER BY budget, id) AS prev_budget,
    LAG(budget, 2) OVER (PARTITION BY location ORDER BY budget, id) AS prev_budget_2
FROM departments
WHERE location NOT IN ('Berlin', 'Chiang Mai');
