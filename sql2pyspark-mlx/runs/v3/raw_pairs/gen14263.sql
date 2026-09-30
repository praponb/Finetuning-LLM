SELECT CASE WHEN budget >= 3468000 THEN 'high' WHEN budget >= 990000 THEN 'mid' ELSE 'low' END AS budget_bucket, SUM(CASE WHEN dept_name = 'Engineering' THEN budget ELSE 0 END) AS engineering_budget
FROM departments
WHERE budget <= 1721000
GROUP BY 1
ORDER BY engineering_budget DESC;
