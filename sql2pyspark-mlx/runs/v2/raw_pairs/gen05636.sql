SELECT
    CASE WHEN budget >= 4469000 THEN 'high' WHEN budget >= 1111000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    location,
    ROUND(STDDEV(budget), 2) AS stddev_budget,
    MAX(budget) AS max_budget,
    MAX(budget) - MIN(budget) AS budget_range
FROM departments
WHERE dept_name != 'HR'
GROUP BY CASE WHEN budget >= 4469000 THEN 'high' WHEN budget >= 1111000 THEN 'mid' ELSE 'low' END, location;
