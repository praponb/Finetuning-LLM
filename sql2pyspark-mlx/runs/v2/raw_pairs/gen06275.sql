SELECT location, SUM(budget) total_budget, DENSE_RANK() OVER (ORDER BY SUM(budget) DESC) AS budget_rank FROM departments GROUP BY location ORDER BY total_budget DESC
