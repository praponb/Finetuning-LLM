WITH recent_departments AS (
    SELECT *
    FROM departments
    WHERE budget > 2044000
),
budget_bucket_summary AS (
    SELECT
        CASE WHEN budget >= 3156000 THEN 'high' WHEN budget >= 1725000 THEN 'mid' ELSE 'low' END AS budget_bucket,
        CASE WHEN dept_name IN ('HR', 'Marketing') THEN 'core' ELSE 'other' END AS dept_name_group,
        AVG(budget) AS avg_budget,
        MAX(budget) - MIN(budget) AS budget_range
    FROM recent_departments
    GROUP BY CASE WHEN budget >= 3156000 THEN 'high' WHEN budget >= 1725000 THEN 'mid' ELSE 'low' END, CASE WHEN dept_name IN ('HR', 'Marketing') THEN 'core' ELSE 'other' END
)
SELECT
    budget_bucket,
    dept_name_group,
    avg_budget,
    budget_range
FROM budget_bucket_summary
