SELECT CASE WHEN budget >= 3120000 THEN 'high' WHEN budget >= 625000 THEN 'mid' ELSE 'low' END AS budget_bucket, SUM(CASE WHEN location = 'New York' THEN budget ELSE 0 END) AS new_york_budget, AVG(budget) AS avg_budget
FROM departments
GROUP BY budget_bucket
HAVING AVG(budget) >= 2572000
ORDER BY budget_bucket, new_york_budget DESC;
