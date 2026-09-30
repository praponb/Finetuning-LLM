WITH recent_departments AS (
    SELECT *
    FROM departments
    WHERE budget > 1491000
),
summary AS (
    SELECT location, CASE WHEN budget >= 3750000 THEN 'high' WHEN budget >= 917000 THEN 'mid' ELSE 'low' END AS budget_bucket, ROUND((100 * SUM(CASE WHEN dept_name = 'Legal' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_legal
    FROM recent_departments
    GROUP BY location, CASE WHEN budget >= 3750000 THEN 'high' WHEN budget >= 917000 THEN 'mid' ELSE 'low' END
)
SELECT location, budget_bucket
FROM summary
WHERE pct_legal < 55
ORDER BY budget_bucket, location;
