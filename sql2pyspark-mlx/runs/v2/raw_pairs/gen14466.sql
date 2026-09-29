SELECT location, COUNT(budget) AS budget_count
FROM departments
WHERE location IN ('New York', 'Tokyo')
GROUP BY location
HAVING budget_count >= 5;
