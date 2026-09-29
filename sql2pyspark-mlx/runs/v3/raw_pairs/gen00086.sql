WITH valid_departments AS (
    SELECT *
    FROM departments
    WHERE budget IS NOT NULL
),
departments_stats AS (
    SELECT CASE WHEN budget >= 3847000 THEN 'high' WHEN budget >= 1360000 THEN 'mid' ELSE 'low' END AS budget_bucket, SUM(budget) AS total_budget, ROUND(AVG(budget), 2) AS avg_budget
    FROM valid_departments
    GROUP BY CASE WHEN budget >= 3847000 THEN 'high' WHEN budget >= 1360000 THEN 'mid' ELSE 'low' END
)
SELECT budget_bucket, total_budget, avg_budget
FROM departments_stats
WHERE avg_budget >= 40
ORDER BY budget_bucket, avg_budget DESC
