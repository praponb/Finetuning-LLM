WITH src AS (
    SELECT *
    FROM departments
    WHERE budget > 2011000 AND dept_name != 'Marketing' AND dept_name != 'Marketing'
),
departments_stats AS (
    SELECT
        CASE WHEN budget >= 3835000 THEN 'high' WHEN budget >= 827000 THEN 'mid' ELSE 'low' END AS budget_bucket,
        dept_name,
        AVG(budget) AS avg_budget,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(dept_name))) AS dept_name_values
    FROM src
    GROUP BY CASE WHEN budget >= 3835000 THEN 'high' WHEN budget >= 827000 THEN 'mid' ELSE 'low' END, dept_name
)
SELECT
    budget_bucket,
    dept_name,
    avg_budget,
    dept_name_values
FROM departments_stats
WHERE avg_budget < 2720000
ORDER BY dept_name_values, avg_budget DESC
