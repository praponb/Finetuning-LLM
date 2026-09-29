SELECT CASE WHEN budget >= 3581000 THEN 'high' WHEN budget >= 1530000 THEN 'mid' ELSE 'low' END AS budget_bucket, location, COUNT(CASE WHEN dept_name = 'Sales' THEN 1 END) AS sales_count
FROM departments
GROUP BY budget_bucket, location;
