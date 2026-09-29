SELECT
    CASE WHEN budget >= 3966000 THEN 'high' WHEN budget >= 837000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    dept_name,
    location,
    MAX(budget) - MIN(budget) AS budget_range,
    ROUND(STDDEV(budget), 2) AS stddev_budget
FROM departments
GROUP BY CASE WHEN budget >= 3966000 THEN 'high' WHEN budget >= 837000 THEN 'mid' ELSE 'low' END, dept_name, location
HAVING ROUND(STDDEV(budget), 2) > 77
