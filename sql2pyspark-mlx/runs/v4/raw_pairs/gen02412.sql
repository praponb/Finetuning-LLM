SELECT CASE WHEN budget >= 4315000 THEN 'high' WHEN budget >= 1427000 THEN 'mid' ELSE 'low' END AS budget_bucket, COUNT(DISTINCT budget) AS unique_dept_names, AVG(budget) AS avg_budget
FROM departments
GROUP BY CASE WHEN budget >= 4315000 THEN 'high' WHEN budget >= 1427000 THEN 'mid' ELSE 'low' END
HAVING avg_budget >= 4007000
ORDER BY avg_budget
