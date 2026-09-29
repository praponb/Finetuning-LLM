SELECT *
FROM (
    SELECT
        CASE WHEN budget >= 3962000 THEN 'high' WHEN budget >= 2011000 THEN 'mid' ELSE 'low' END AS budget_bucket,
        location,
        MIN(budget) AS min_budget,
        COUNT(CASE WHEN dept_name = 'Legal' THEN 1 END) AS legal_count
    FROM departments
    GROUP BY CASE WHEN budget >= 3962000 THEN 'high' WHEN budget >= 2011000 THEN 'mid' ELSE 'low' END, location
) AS agg
WHERE min_budget > 3011000
ORDER BY legal_count DESC, location
