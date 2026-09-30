SELECT
    id,
    location,
    budget,
    MIN(budget) OVER (PARTITION BY location) location_min_budget
FROM departments
