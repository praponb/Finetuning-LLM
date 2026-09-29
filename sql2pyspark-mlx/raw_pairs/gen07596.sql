WITH valid_departments AS (
    SELECT *, SUBSTRING(dept_name, 1, 4) AS dept_name_prefix
    FROM departments
    WHERE location IN ('London', 'Tokyo') OR dept_name IS NULL
),
departments_stats AS (
    SELECT
        dept_name,
        CASE WHEN budget >= 3984000 THEN 'high' WHEN budget >= 1865000 THEN 'mid' ELSE 'low' END AS budget_bucket,
        COUNT(*) AS total_count,
        MIN(budget) AS min_budget
    FROM valid_departments
    GROUP BY dept_name, CASE WHEN budget >= 3984000 THEN 'high' WHEN budget >= 1865000 THEN 'mid' ELSE 'low' END
)
SELECT
    dept_name,
    budget_bucket,
    total_count,
    min_budget
FROM departments_stats
WHERE min_budget < 1976000
ORDER BY total_count DESC
