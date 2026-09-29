SELECT
    CASE WHEN budget >= 4372000 THEN 'high' WHEN budget >= 1893000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    SUM(CASE WHEN dept_name = 'Marketing' THEN budget ELSE 0 END) AS marketing_budget,
    COUNT(DISTINCT budget) AS unique_dept_names,
    ROUND(AVG(budget), 2) AS avg_budget
FROM departments
GROUP BY 1
ORDER BY budget_bucket, 4 DESC
