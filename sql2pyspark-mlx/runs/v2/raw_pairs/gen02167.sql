WITH base AS (
    SELECT *
    FROM departments
    WHERE dept_name NOT IN ('HR', 'Legal', 'Sales') AND dept_name NOT IN ('Finance', 'Engineering', 'Legal')
),
departments_stats AS (
    SELECT
        location,
        CASE WHEN budget >= 3117000 THEN 'high' WHEN budget >= 1630000 THEN 'mid' ELSE 'low' END AS budget_bucket,
        SUM(CASE WHEN location = 'London' THEN budget ELSE 0 END) AS london_budget,
        AVG(budget) AS avg_budget
    FROM base
    GROUP BY location, CASE WHEN budget >= 3117000 THEN 'high' WHEN budget >= 1630000 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM departments_stats
WHERE avg_budget > 3503000
ORDER BY location;
