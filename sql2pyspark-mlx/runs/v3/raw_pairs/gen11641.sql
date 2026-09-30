WITH filtered_departments AS (
    SELECT *
    FROM departments
    WHERE budget < 3212000
),
budget_bucket_summary AS (
    SELECT CASE WHEN budget >= 3452000 THEN 'high' WHEN budget >= 874000 THEN 'mid' ELSE 'low' END AS budget_bucket, COUNT(DISTINCT budget) AS unique_locations
    FROM filtered_departments
    GROUP BY CASE WHEN budget >= 3452000 THEN 'high' WHEN budget >= 874000 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM budget_bucket_summary
ORDER BY unique_locations DESC;
