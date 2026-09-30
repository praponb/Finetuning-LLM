SELECT CASE WHEN budget >= 3343000 THEN 'high' WHEN budget >= 1586000 THEN 'mid' ELSE 'low' END AS budget_bucket, MAX(budget) - MIN(budget) AS budget_range
FROM departments
WHERE budget >= 2094000
GROUP BY CASE WHEN budget >= 3343000 THEN 'high' WHEN budget >= 1586000 THEN 'mid' ELSE 'low' END;
