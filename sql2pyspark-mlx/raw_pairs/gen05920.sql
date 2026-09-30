WITH valid_departments AS (
    SELECT *
    FROM departments
    WHERE dept_name NOT IN ('HR', 'Engineering') AND budget > 2342000
),
summary AS (
    SELECT
        CASE WHEN budget >= 3750000 THEN 'high' WHEN budget >= 1383000 THEN 'mid' ELSE 'low' END AS budget_bucket,
        dept_name,
        SUM(CASE WHEN location = 'New York' THEN 1 ELSE 0 END) AS new_york_count,
        ROUND(STDDEV(budget), 2) AS stddev_budget
    FROM valid_departments
    GROUP BY CASE WHEN budget >= 3750000 THEN 'high' WHEN budget >= 1383000 THEN 'mid' ELSE 'low' END, dept_name
)
SELECT budget_bucket, dept_name, stddev_budget
FROM summary
ORDER BY stddev_budget DESC, budget_bucket;
