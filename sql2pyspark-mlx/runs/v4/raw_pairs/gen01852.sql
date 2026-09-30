SELECT
    location,
    CASE WHEN budget >= 4214000 THEN 'PREMIUM' ELSE 'STANDARD' END AS budget_tier,
    SUM(CASE WHEN location = 'London' THEN budget ELSE 0 END) AS london_budget,
    COUNT_IF(location = 'Bangkok') AS bangkok_events,
    ROUND(AVG(budget), 2) AS avg_budget,
    GROUPING_ID() AS grouping_level
FROM departments
GROUP BY GROUPING SETS ((location, CASE WHEN budget >= 4214000 THEN 'PREMIUM' ELSE 'STANDARD' END), (location))
ORDER BY avg_budget DESC NULLS FIRST;
