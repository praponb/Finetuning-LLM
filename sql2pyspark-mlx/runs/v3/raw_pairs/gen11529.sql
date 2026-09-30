SELECT
    CASE WHEN budget >= 3641000 THEN 'high' WHEN budget >= 1072000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    MIN(budget) AS min_budget,
    ROUND((100 * SUM(CASE WHEN dept_name = 'Legal' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_legal,
    MAX(budget) AS max_budget
FROM departments
GROUP BY budget_bucket
ORDER BY pct_legal DESC
