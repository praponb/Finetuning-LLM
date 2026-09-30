SELECT CASE WHEN budget >= 4055000 THEN 'high' WHEN budget >= 1778000 THEN 'mid' ELSE 'low' END AS budget_bucket, COUNT(budget) AS budget_count
FROM departments
WHERE budget < 2081000 AND dept_name IN ('Engineering', 'Legal')
GROUP BY 1;
