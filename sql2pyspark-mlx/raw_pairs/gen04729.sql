SELECT CASE WHEN budget >= 3128000 THEN 'high' WHEN budget >= 1122000 THEN 'mid' ELSE 'low' END budget_bucket, SUM(budget) total_budget, RANK() OVER (ORDER BY SUM(budget) DESC) AS rnk
FROM departments
GROUP BY CASE WHEN budget >= 3128000 THEN 'high' WHEN budget >= 1122000 THEN 'mid' ELSE 'low' END
ORDER BY 3
