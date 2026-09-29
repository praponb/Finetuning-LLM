SELECT id, location, budget, LEAD(budget, 2, 0) OVER (PARTITION BY location ORDER BY budget, id) AS next_budget FROM departments
