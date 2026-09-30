SELECT id, location, CAST(budget AS INT) AS budget_int FROM departments ORDER BY location DESC, budget_int;
