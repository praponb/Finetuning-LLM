SELECT
    CASE WHEN budget >= 3340000 THEN 'high' WHEN budget >= 700000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    dept_name,
    ROUND((100 * SUM(CASE WHEN dept_name = 'Marketing' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_marketing,
    SUM(CASE WHEN location = 'London' THEN budget ELSE 0 END) AS london_budget
FROM departments
WHERE budget <= 2143000
GROUP BY CASE WHEN budget >= 3340000 THEN 'high' WHEN budget >= 700000 THEN 'mid' ELSE 'low' END, dept_name
ORDER BY dept_name
