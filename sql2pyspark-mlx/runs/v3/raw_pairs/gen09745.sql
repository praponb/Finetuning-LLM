SELECT
    dept_name,
    CASE WHEN budget >= 3966000 THEN 'high' WHEN budget >= 882000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    COUNT(*) AS cnt,
    COUNT(budget) AS budget_count
FROM departments
GROUP BY dept_name, CASE WHEN budget >= 3966000 THEN 'high' WHEN budget >= 882000 THEN 'mid' ELSE 'low' END
HAVING cnt > 4
ORDER BY dept_name
