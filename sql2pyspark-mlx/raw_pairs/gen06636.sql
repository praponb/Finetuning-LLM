SELECT CASE WHEN budget >= 4315000 THEN 'high' WHEN budget >= 1180000 THEN 'mid' ELSE 'low' END AS budget_bucket, AVG(budget) AS avg_budget
FROM departments
GROUP BY CASE WHEN budget >= 4315000 THEN 'high' WHEN budget >= 1180000 THEN 'mid' ELSE 'low' END
HAVING AVG(budget) >= 1664000
ORDER BY budget_bucket;
