SELECT
    id,
    dept_name,
    budget,
    location,
    CEIL(budget) AS budget_whole
FROM departments
WHERE location NOT IN ('Chiang Mai', 'Tokyo') AND location <> 'Bangkok';
