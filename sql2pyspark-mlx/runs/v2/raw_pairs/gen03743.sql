SELECT
    id,
    location,
    budget,
    LEAD(budget, 1) OVER (PARTITION BY location ORDER BY budget, id) next_budget
FROM departments
WHERE budget > 1313000;
