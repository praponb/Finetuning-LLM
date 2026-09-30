SELECT location, SUM(budget) AS total_budget, DENSE_RANK() OVER (ORDER BY SUM(budget) DESC) AS budget_rank FROM departments WHERE location NOT IN ('Singapore', 'Chiang Mai') GROUP BY location;
