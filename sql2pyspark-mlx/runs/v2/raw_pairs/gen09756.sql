SELECT location, SUM(budget) AS total_budget, COUNT(*) AS cnt, RANK() OVER (ORDER BY SUM(budget) DESC) AS rnk FROM departments GROUP BY location ORDER BY total_budget, rnk DESC, location LIMIT 1
