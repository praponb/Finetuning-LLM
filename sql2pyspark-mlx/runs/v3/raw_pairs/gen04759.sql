WITH base AS (
    SELECT *
    FROM departments
    WHERE location NOT IN ('Berlin', 'Singapore', 'London')
),
budget_bucket_summary AS (
    SELECT CASE WHEN budget >= 3496000 THEN 'high' WHEN budget >= 1446000 THEN 'mid' ELSE 'low' END AS budget_bucket, SUM(budget) AS total_budget, COUNT(*) AS total_count
    FROM base
    GROUP BY CASE WHEN budget >= 3496000 THEN 'high' WHEN budget >= 1446000 THEN 'mid' ELSE 'low' END
)
SELECT budget_bucket, total_budget, total_count
FROM budget_bucket_summary
