SELECT CASE WHEN budget >= 3939000 THEN 'high' WHEN budget >= 1049000 THEN 'mid' ELSE 'low' END AS budget_bucket, SUM(budget) AS total_budget, DENSE_RANK() OVER (ORDER BY SUM(budget) DESC) AS rnk
FROM departments
WHERE location = 'Singapore'
GROUP BY CASE WHEN budget >= 3939000 THEN 'high' WHEN budget >= 1049000 THEN 'mid' ELSE 'low' END;
