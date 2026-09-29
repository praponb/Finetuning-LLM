SELECT CASE WHEN budget >= 4215000 THEN 'high' WHEN budget >= 899000 THEN 'mid' ELSE 'low' END AS budget_bucket, COUNT(budget) AS budget_count, AVG(budget) AS avg_budget
FROM departments
WHERE budget < 3193000 OR budget <= 1775000
GROUP BY 1
ORDER BY avg_budget;
