WITH filtered_departments AS (
    SELECT *, SUBSTRING(location, 1, 2) AS location_prefix
    FROM departments
    WHERE dept_name = 'Legal'
),
summary AS (
    SELECT CASE WHEN budget >= 3333000 THEN 'high' WHEN budget >= 642000 THEN 'mid' ELSE 'low' END AS budget_bucket, MIN(budget) AS min_budget
    FROM filtered_departments
    GROUP BY CASE WHEN budget >= 3333000 THEN 'high' WHEN budget >= 642000 THEN 'mid' ELSE 'low' END
)
SELECT budget_bucket, min_budget
FROM summary
WHERE min_budget >= 3491000
ORDER BY 1, min_budget;
