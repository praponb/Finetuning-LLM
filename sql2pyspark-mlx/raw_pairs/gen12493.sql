SELECT
    CASE WHEN budget >= 4400000 THEN 'high' WHEN budget >= 1302000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    location,
    MIN(budget) AS min_budget,
    SUM(budget) AS total_budget
FROM departments
GROUP BY CASE WHEN budget >= 4400000 THEN 'high' WHEN budget >= 1302000 THEN 'mid' ELSE 'low' END, location
HAVING SUM(budget) >= 21837000
ORDER BY total_budget;
