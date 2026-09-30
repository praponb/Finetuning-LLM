WITH filtered_departments AS (
    SELECT *
    FROM departments
    WHERE dept_name IN ('Legal', 'Marketing', 'Sales') AND location IN ('New York', 'London', 'Berlin')
),
budget_bucket_summary AS (
    SELECT CASE WHEN budget >= 4013000 THEN 'high' WHEN budget >= 1433000 THEN 'mid' ELSE 'low' END AS budget_bucket, AVG(budget) AS avg_budget, COUNT(DISTINCT budget) AS unique_locations
    FROM filtered_departments
    GROUP BY CASE WHEN budget >= 4013000 THEN 'high' WHEN budget >= 1433000 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM budget_bucket_summary;
