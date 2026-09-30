SELECT
    CASE WHEN budget >= 4367000 THEN 'high' WHEN budget >= 1229000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    dept_name,
    location,
    SUM(CASE WHEN dept_name = 'Finance' THEN 1 ELSE 0 END) AS finance_count,
    ROUND((100 * SUM(CASE WHEN dept_name = 'HR' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_hr,
    ROUND(AVG(budget), 2) AS avg_budget
FROM departments
GROUP BY CASE WHEN budget >= 4367000 THEN 'high' WHEN budget >= 1229000 THEN 'mid' ELSE 'low' END, dept_name, location
HAVING ROUND(AVG(budget), 2) >= 37
ORDER BY pct_hr, finance_count, budget_bucket, dept_name, location
LIMIT 5
