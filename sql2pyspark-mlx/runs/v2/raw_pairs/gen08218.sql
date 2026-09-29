SELECT
    id,
    location,
    budget,
    LAG(budget, 2) OVER (PARTITION BY location ORDER BY budget, id) AS prev_budget
FROM departments
ORDER BY location, budget
