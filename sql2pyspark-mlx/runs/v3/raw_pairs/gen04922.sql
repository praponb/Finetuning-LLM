SELECT
    dept_name,
    CASE WHEN budget >= 4459000 THEN 'high' WHEN budget >= 615000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    ROUND(STDDEV(budget), 2) AS stddev_budget,
    SUM(CASE WHEN location = 'London' THEN budget ELSE 0 END) AS london_budget
FROM departments
WHERE dept_name IN ('Legal', 'Finance') AND location = 'London'
GROUP BY dept_name, CASE WHEN budget >= 4459000 THEN 'high' WHEN budget >= 615000 THEN 'mid' ELSE 'low' END
ORDER BY 1, budget_bucket
LIMIT 5;
