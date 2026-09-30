SELECT CASE WHEN budget >= 3964000 THEN 'high' WHEN budget >= 1864000 THEN 'mid' ELSE 'low' END AS budget_bucket, MAX(budget) AS max_budget, MAX(budget) - MIN(budget) AS budget_range
FROM departments
GROUP BY 1
HAVING MAX(budget) - MIN(budget) >= 51
ORDER BY budget_range DESC
