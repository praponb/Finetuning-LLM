SELECT id, location, budget, RANK() OVER (PARTITION BY location ORDER BY budget) AS budget_rank FROM departments WHERE budget <= 2437000
