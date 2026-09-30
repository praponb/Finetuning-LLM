SELECT CASE WHEN budget >= 3798000 THEN 'high' WHEN budget >= 1671000 THEN 'mid' ELSE 'low' END AS budget_bucket, COUNT(CASE WHEN location = 'New York' THEN 1 END) AS new_york_count
FROM departments
WHERE dept_name IN ('Legal', 'HR')
GROUP BY CASE WHEN budget >= 3798000 THEN 'high' WHEN budget >= 1671000 THEN 'mid' ELSE 'low' END;
