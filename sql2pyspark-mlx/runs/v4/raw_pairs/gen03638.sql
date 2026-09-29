SELECT
    CASE WHEN budget >= 4489000 THEN 'high' WHEN budget >= 1880000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    location,
    COUNT(budget) AS budget_count,
    SUM(budget) AS total_budget,
    ROUND(AVG(budget), 2) AS avg_budget
FROM departments
GROUP BY CASE WHEN budget >= 4489000 THEN 'high' WHEN budget >= 1880000 THEN 'mid' ELSE 'low' END, location
ORDER BY avg_budget, budget_bucket DESC, location
LIMIT 1
