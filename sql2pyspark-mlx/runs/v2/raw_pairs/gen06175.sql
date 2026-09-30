SELECT CASE WHEN budget >= 4241000 THEN 'high' WHEN budget >= 1449000 THEN 'mid' ELSE 'low' END AS budget_bucket, MIN(budget) AS min_budget
FROM departments
GROUP BY CASE WHEN budget >= 4241000 THEN 'high' WHEN budget >= 1449000 THEN 'mid' ELSE 'low' END
ORDER BY min_budget
