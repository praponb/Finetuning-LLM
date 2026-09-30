SELECT *, COALESCE(budget, 0) AS budget_filled
FROM departments
