SELECT CASE WHEN budget >= 3332000 THEN 'high' WHEN budget >= 1769000 THEN 'mid' ELSE 'low' END AS budget_bucket, COUNT(CASE WHEN location = 'New York' THEN 1 END) AS new_york_count
FROM departments
WHERE budget <= 1990000 AND (budget <= 2014000 OR budget > 1253000)
GROUP BY budget_bucket
ORDER BY 2 DESC;
