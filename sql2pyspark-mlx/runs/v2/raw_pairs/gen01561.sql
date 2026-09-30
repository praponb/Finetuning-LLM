SELECT CASE WHEN budget >= 4367000 THEN 'high' WHEN budget >= 1779000 THEN 'mid' ELSE 'low' END AS budget_bucket, COUNT(DISTINCT budget) AS unique_dept_names
FROM departments
WHERE location <> 'Bangkok'
GROUP BY budget_bucket
ORDER BY unique_dept_names DESC, budget_bucket
LIMIT 50
