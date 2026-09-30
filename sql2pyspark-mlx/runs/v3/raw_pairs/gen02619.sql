SELECT location, AVG(budget) AS avg_budget
FROM departments
GROUP BY 1
ORDER BY location
LIMIT 10
