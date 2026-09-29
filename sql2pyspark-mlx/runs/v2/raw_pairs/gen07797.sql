WITH base AS (
    SELECT *
    FROM departments
    WHERE dept_name <> 'Marketing' AND location = 'Chiang Mai'
),
budget_bucket_summary AS (
    SELECT
        CASE WHEN budget >= 3714000 THEN 'high' WHEN budget >= 1659000 THEN 'mid' ELSE 'low' END AS budget_bucket,
        location,
        COUNT(*) AS num_departments,
        MAX(budget) AS max_budget
    FROM base
    GROUP BY CASE WHEN budget >= 3714000 THEN 'high' WHEN budget >= 1659000 THEN 'mid' ELSE 'low' END, location
)
SELECT *
FROM budget_bucket_summary;
