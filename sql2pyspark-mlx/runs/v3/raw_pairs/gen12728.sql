SELECT id, location, budget, budget - LAG(budget, 1) OVER (PARTITION BY location ORDER BY budget, id) AS budget_change FROM departments WHERE budget IS NOT NULL;
