SELECT
    CASE WHEN budget >= 3081000 THEN 'high' WHEN budget >= 1546000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    SUM(budget) AS total_budget,
    COUNT(*) AS cnt,
    SUM(SUM(budget)) OVER (ORDER BY CASE WHEN budget >= 3081000 THEN 'high' WHEN budget >= 1546000 THEN 'mid' ELSE 'low' END) AS cumulative_budget
FROM departments
WHERE budget > 3744000
GROUP BY CASE WHEN budget >= 3081000 THEN 'high' WHEN budget >= 1546000 THEN 'mid' ELSE 'low' END
ORDER BY budget_bucket DESC, cumulative_budget DESC;
