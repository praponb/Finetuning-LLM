SELECT location, SUM(budget) total_budget, RANK() OVER (ORDER BY SUM(budget) DESC) AS rnk FROM departments GROUP BY location ORDER BY total_budget, location LIMIT 1;
