SELECT
    CASE WHEN budget >= 3298000 THEN 'high' WHEN budget >= 656000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    location,
    COUNT(*) AS cnt,
    ROUND(STDDEV(budget), 2) AS stddev_budget,
    SUM(CASE WHEN dept_name = 'Legal' THEN budget ELSE 0 END) AS legal_budget
FROM departments
GROUP BY CUBE(CASE WHEN budget >= 3298000 THEN 'high' WHEN budget >= 656000 THEN 'mid' ELSE 'low' END, location);
