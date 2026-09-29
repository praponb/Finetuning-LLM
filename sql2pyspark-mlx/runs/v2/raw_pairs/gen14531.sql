SELECT AVG(budget) AS avg_budget
FROM departments
WHERE location IN ('London', 'Singapore', 'New York')
ORDER BY avg_budget DESC;
