SELECT
    CASE WHEN budget >= 4213000 THEN 'LARGE' ELSE 'SMALL' END AS budget_tier,
    SUM(CASE WHEN dept_name = 'Marketing' THEN budget ELSE 0 END) AS marketing_budget,
    PERCENTILE_APPROX(budget, 0.95) AS budget_p95,
    COUNT_IF(dept_name = 'Engineering') AS engineering_events,
    COUNT_IF(location = 'Bangkok') AS bangkok_events,
    COUNT(CASE WHEN location = 'Berlin' THEN 1 END) AS berlin_count
FROM departments
GROUP BY CASE WHEN budget >= 4213000 THEN 'LARGE' ELSE 'SMALL' END
HAVING COUNT(CASE WHEN location = 'Berlin' THEN 1 END) < 1
