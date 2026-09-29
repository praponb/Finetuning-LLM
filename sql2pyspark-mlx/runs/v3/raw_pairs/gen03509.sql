SELECT CASE WHEN budget >= 3713000 THEN 'high' WHEN budget >= 1672000 THEN 'mid' ELSE 'low' END AS budget_bucket, location, ROUND(STDDEV(budget), 2) AS stddev_budget
FROM departments
GROUP BY CASE WHEN budget >= 3713000 THEN 'high' WHEN budget >= 1672000 THEN 'mid' ELSE 'low' END, location
ORDER BY budget_bucket;
