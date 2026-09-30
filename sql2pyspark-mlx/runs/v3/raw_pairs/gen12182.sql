WITH filtered_departments AS (
    SELECT *
    FROM departments
    WHERE location = 'Tokyo' AND (dept_name = 'HR' OR location IN ('Berlin', 'London'))
),
DeptNameSummary AS (
    SELECT
        dept_name,
        CASE WHEN budget >= 4410000 THEN 'high' WHEN budget >= 1412000 THEN 'mid' ELSE 'low' END AS budget_bucket,
        SUM(CASE WHEN dept_name = 'Engineering' THEN budget ELSE 0 END) AS engineering_budget,
        COUNT(*) AS department_count
    FROM filtered_departments
    GROUP BY dept_name, CASE WHEN budget >= 4410000 THEN 'high' WHEN budget >= 1412000 THEN 'mid' ELSE 'low' END
)
SELECT dept_name, budget_bucket, engineering_budget
FROM DeptNameSummary
ORDER BY engineering_budget, dept_name, budget_bucket
LIMIT 3
