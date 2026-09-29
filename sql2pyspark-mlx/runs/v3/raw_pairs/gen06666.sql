SELECT CASE WHEN budget >= 4074000 THEN 'high' WHEN budget >= 1597000 THEN 'mid' ELSE 'low' END budget_bucket, SUM(budget) total_budget, ROUND(SUM(budget) * 100.0 / SUM(SUM(budget)) OVER (), 2) AS pct_of_total
FROM departments
GROUP BY CASE WHEN budget >= 4074000 THEN 'high' WHEN budget >= 1597000 THEN 'mid' ELSE 'low' END
ORDER BY 1
