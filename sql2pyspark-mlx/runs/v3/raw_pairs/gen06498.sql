SELECT CASE WHEN budget >= 3814000 THEN 'high' WHEN budget >= 1377000 THEN 'mid' ELSE 'low' END AS budget_bucket, dept_name, ROUND(AVG(budget), 2) AS avg_budget
FROM departments
WHERE dept_name IS NULL AND dept_name IN ('Marketing', 'Legal', 'Sales')
GROUP BY 1, 2
ORDER BY budget_bucket
