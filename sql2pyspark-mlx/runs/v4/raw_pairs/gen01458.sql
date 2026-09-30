SELECT
    CASE WHEN budget >= 3247000 THEN 'high' WHEN budget >= 1103000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    dept_name,
    location,
    AVG(budget) AS avg_budget
FROM departments
GROUP BY 1, 2, 3
ORDER BY avg_budget DESC
