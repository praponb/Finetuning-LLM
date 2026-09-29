SELECT location, SUM(budget) total_budget, DENSE_RANK() OVER (ORDER BY SUM(budget) DESC) AS rnk
FROM departments
GROUP BY location
ORDER BY location DESC;
