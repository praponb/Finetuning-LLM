SELECT CASE WHEN budget >= 4149000 THEN 'high' WHEN budget >= 1881000 THEN 'mid' ELSE 'low' END AS budget_bucket, SUM(budget) AS total_budget, SUM(CASE WHEN dept_name = 'Sales' THEN budget ELSE 0 END) AS sales_budget
FROM departments
WHERE budget <= 2428000
GROUP BY budget_bucket
HAVING SUM(CASE WHEN dept_name = 'Sales' THEN budget ELSE 0 END) < 55;
