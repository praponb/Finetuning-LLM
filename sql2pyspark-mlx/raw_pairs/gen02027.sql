SELECT
    dept_name,
    CASE WHEN budget >= 3446000 THEN 'high' WHEN budget >= 1511000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    AVG(budget) AS avg_budget,
    MAX(budget) - MIN(budget) AS budget_range
FROM departments
WHERE location = 'Singapore'
GROUP BY dept_name, CASE WHEN budget >= 3446000 THEN 'high' WHEN budget >= 1511000 THEN 'mid' ELSE 'low' END
ORDER BY dept_name DESC;
