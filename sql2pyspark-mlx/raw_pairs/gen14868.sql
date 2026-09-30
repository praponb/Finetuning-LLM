SELECT location, ROUND(AVG(budget), 2) AS avg_budget, COUNT(budget) AS budget_count FROM departments GROUP BY location
