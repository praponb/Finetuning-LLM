SELECT
    id,
    dept_name,
    budget,
    location,
    LOWER(dept_name) AS dept_name_lower,
    ROUND(budget, 0) AS budget_rounded
FROM departments
WHERE location = 'Singapore' AND dept_name <> 'Marketing';
