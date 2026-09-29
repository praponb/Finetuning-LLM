WITH filtered_departments AS (
    SELECT *
    FROM departments
    WHERE dept_name = 'Engineering' AND (budget > 2557000 OR dept_name IN ('HR', 'Sales'))
),
DepartmentsStats AS (
    SELECT CASE WHEN budget >= 3040000 THEN 'high' WHEN budget >= 1572000 THEN 'mid' ELSE 'low' END AS budget_bucket, location, MIN(budget) AS min_budget
    FROM filtered_departments
    GROUP BY CASE WHEN budget >= 3040000 THEN 'high' WHEN budget >= 1572000 THEN 'mid' ELSE 'low' END, location
)
SELECT budget_bucket, min_budget
FROM DepartmentsStats
WHERE min_budget > 1144000
ORDER BY budget_bucket DESC
