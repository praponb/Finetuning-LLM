SELECT CASE WHEN budget >= 4022000 THEN 'high' WHEN budget >= 1556000 THEN 'mid' ELSE 'low' END AS budget_bucket, ROUND(AVG(budget), 2) AS avg_budget, COUNT(CASE WHEN dept_name = 'Finance' THEN 1 END) AS finance_count
FROM departments
WHERE dept_name = 'Sales' OR location IN ('New York', 'Tokyo', 'Bangkok')
GROUP BY CASE WHEN budget >= 4022000 THEN 'high' WHEN budget >= 1556000 THEN 'mid' ELSE 'low' END
