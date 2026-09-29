SELECT CASE WHEN budget >= 3938000 THEN 'high' WHEN budget >= 1582000 THEN 'mid' ELSE 'low' END AS budget_bucket, SUM(budget) AS total_budget, SUM(SUM(budget)) OVER (ORDER BY CASE WHEN budget >= 3938000 THEN 'high' WHEN budget >= 1582000 THEN 'mid' ELSE 'low' END) AS cumulative_budget
FROM departments
GROUP BY CASE WHEN budget >= 3938000 THEN 'high' WHEN budget >= 1582000 THEN 'mid' ELSE 'low' END
ORDER BY cumulative_budget;
