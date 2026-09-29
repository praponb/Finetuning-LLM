SELECT
    id,
    dept_name,
    location,
    ROUND(budget, 2) AS budget_rounded,
    COALESCE(location, 'unassigned') AS location_filled
FROM departments
WHERE budget > 2813000 AND dept_name = 'HR'
