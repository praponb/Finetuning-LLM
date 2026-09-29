SELECT
    location,
    CASE WHEN budget >= 3303000 THEN 'high' WHEN budget >= 904000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    ROUND(AVG(budget), 2) AS avg_budget,
    MAX(budget) - MIN(budget) AS budget_range
FROM departments
WHERE dept_name = 'Finance'
GROUP BY 1, 2
HAVING ROUND(AVG(budget), 2) > 42;
