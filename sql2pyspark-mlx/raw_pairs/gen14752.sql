SELECT budget, CAST(budget AS INT) AS budget_int, COALESCE(budget, 0) AS budget_filled
FROM departments;
