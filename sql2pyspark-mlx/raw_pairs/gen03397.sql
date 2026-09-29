SELECT
    CASE WHEN budget >= 4219000 THEN 'high' WHEN budget >= 665000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    CASE WHEN location IN ('New York', 'London') THEN 'priority' ELSE 'other' END AS location_group,
    SUM(CASE WHEN dept_name = 'HR' THEN budget ELSE 0 END) AS hr_budget,
    ROUND(AVG(budget), 2) AS avg_budget
FROM departments
WHERE dept_name IS NOT NULL
GROUP BY CUBE(CASE WHEN budget >= 4219000 THEN 'high' WHEN budget >= 665000 THEN 'mid' ELSE 'low' END, CASE WHEN location IN ('New York', 'London') THEN 'priority' ELSE 'other' END)
ORDER BY budget_bucket;
