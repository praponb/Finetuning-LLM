SELECT CASE WHEN budget >= 3501000 THEN 'high' WHEN budget >= 927000 THEN 'mid' ELSE 'low' END AS budget_bucket, AVG(budget) AS avg_budget, MIN(budget) AS min_budget
FROM departments
GROUP BY 1
ORDER BY budget_bucket DESC
LIMIT 1;
