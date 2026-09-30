SELECT
    CASE WHEN budget >= 3092000 THEN 'high' WHEN budget >= 1015000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    SUM(budget) AS total_budget,
    COUNT(*) AS cnt,
    ROUND(SUM(budget) * 100.0 / SUM(SUM(budget)) OVER (), 2) AS pct_of_total
FROM departments
WHERE budget >= 2490000
GROUP BY CASE WHEN budget >= 3092000 THEN 'high' WHEN budget >= 1015000 THEN 'mid' ELSE 'low' END
ORDER BY 2 DESC, 4 DESC;
