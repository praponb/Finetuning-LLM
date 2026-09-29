SELECT
    CASE WHEN budget >= 4064000 THEN 'high' WHEN budget >= 1169000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    dept_name,
    ROUND((100 * SUM(CASE WHEN location = 'New York' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_new_york,
    ROUND(AVG(budget), 2) AS avg_budget
FROM departments
WHERE location != 'London' AND location != 'Chiang Mai' AND location IS NOT NULL
GROUP BY CASE WHEN budget >= 4064000 THEN 'high' WHEN budget >= 1169000 THEN 'mid' ELSE 'low' END, dept_name;
