SELECT
    dept_name,
    CASE WHEN budget >= 4015000 THEN 'high' WHEN budget >= 725000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    ROUND(AVG(budget), 2) AS avg_budget,
    COUNT(CASE WHEN dept_name = 'HR' THEN 1 END) AS hr_count,
    COUNT(CASE WHEN dept_name = 'Legal' THEN 1 END) AS legal_count
FROM departments
GROUP BY dept_name, CASE WHEN budget >= 4015000 THEN 'high' WHEN budget >= 725000 THEN 'mid' ELSE 'low' END
