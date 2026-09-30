SELECT
    id,
    dept_name,
    budget,
    location,
    COALESCE(budget, 0) AS budget_filled
FROM departments;
