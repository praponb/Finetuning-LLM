SELECT
    id,
    location,
    budget,
    LEAD(budget, 1, 0) OVER (PARTITION BY location ORDER BY budget, id) AS next_budget
FROM departments
