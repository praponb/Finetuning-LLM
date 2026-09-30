SELECT location, SUM(budget) AS total_budget, COUNT(*) AS cnt, RANK() OVER (ORDER BY SUM(budget) DESC) AS budget_rank FROM departments GROUP BY location
