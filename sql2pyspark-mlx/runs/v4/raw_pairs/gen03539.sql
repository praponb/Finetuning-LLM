SELECT
    CASE WHEN budget >= 4390000 THEN 'high' WHEN budget >= 1271000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    SUM(budget) AS total_budget,
    COUNT(*) AS cnt,
    ROUND(SUM(budget) * 100.0 / SUM(SUM(budget)) OVER (), 2) AS pct_of_total
FROM departments
WHERE budget < 3689000
GROUP BY CASE WHEN budget >= 4390000 THEN 'high' WHEN budget >= 1271000 THEN 'mid' ELSE 'low' END
ORDER BY cnt DESC, budget_bucket DESC;
