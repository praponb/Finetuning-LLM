WITH recent_departments AS (
    SELECT *
    FROM departments
    WHERE dept_name = 'Finance'
),
agg AS (
    SELECT
        location,
        CASE WHEN budget >= 3104000 THEN 'high' WHEN budget >= 1433000 THEN 'mid' ELSE 'low' END AS budget_bucket,
        SUM(budget) AS total_budget,
        MAX(budget) AS max_budget
    FROM recent_departments
    GROUP BY location, CASE WHEN budget >= 3104000 THEN 'high' WHEN budget >= 1433000 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM agg
ORDER BY max_budget DESC, location, budget_bucket
LIMIT 10
