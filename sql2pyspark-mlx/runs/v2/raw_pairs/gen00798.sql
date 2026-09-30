SELECT CASE WHEN budget >= 3571000 THEN 'high' WHEN budget >= 2051000 THEN 'mid' ELSE 'low' END AS budget_bucket, MAX(budget) - MIN(budget) AS budget_range
FROM departments
WHERE location = 'Singapore' AND location IN ('Tokyo', 'New York', 'Bangkok')
GROUP BY CASE WHEN budget >= 3571000 THEN 'high' WHEN budget >= 2051000 THEN 'mid' ELSE 'low' END
ORDER BY budget_range DESC;
