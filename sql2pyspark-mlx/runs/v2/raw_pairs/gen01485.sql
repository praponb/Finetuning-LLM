SELECT
    CASE WHEN budget >= 4048000 THEN 'high' WHEN budget >= 1029000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    location,
    COUNT(budget) AS budget_count,
    COUNT(DISTINCT budget) AS unique_dept_names,
    COUNT(CASE WHEN location = 'Singapore' THEN 1 END) AS singapore_count
FROM departments
WHERE dept_name = 'Marketing' OR location NOT IN ('London', 'Tokyo', 'Berlin')
GROUP BY 1, 2
