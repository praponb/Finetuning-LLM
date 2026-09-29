SELECT
    id,
    dept_name,
    location,
    budget,
    COALESCE(budget, 0) AS budget_filled
FROM departments
WHERE budget BETWEEN 1193000 AND 2531000 OR dept_name = 'Engineering'
