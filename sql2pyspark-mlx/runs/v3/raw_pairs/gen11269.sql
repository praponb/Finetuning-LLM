SELECT
    id,
    location,
    dept_name,
    budget,
    CAST(budget AS INT) AS budget_int
FROM departments
WHERE budget < 1793000
