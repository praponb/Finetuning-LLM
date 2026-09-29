SELECT location, SUM(budget) AS total_budget, MAX(budget) AS max_budget
FROM departments
GROUP BY location
HAVING SUM(budget) >= 16932000
ORDER BY 3 DESC;
