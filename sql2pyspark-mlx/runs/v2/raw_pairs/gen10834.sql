SELECT location, AVG(budget) AS avg_budget, COUNT(*) AS cnt
FROM departments
GROUP BY location
ORDER BY cnt DESC, location
LIMIT 20
