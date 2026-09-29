SELECT location, SUM(budget) AS total_budget, SUM(SUM(budget)) OVER (ORDER BY location) AS cumulative_budget FROM departments GROUP BY location
