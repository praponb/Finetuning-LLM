SELECT location, CASE WHEN budget >= 3071000 THEN 'high' WHEN budget >= 1651000 THEN 'mid' ELSE 'low' END AS budget_bucket, ROUND((100 * SUM(CASE WHEN location = 'London' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_london
FROM departments
WHERE dept_name NOT IN ('Sales', 'Legal')
GROUP BY CUBE(location, CASE WHEN budget >= 3071000 THEN 'high' WHEN budget >= 1651000 THEN 'mid' ELSE 'low' END);
