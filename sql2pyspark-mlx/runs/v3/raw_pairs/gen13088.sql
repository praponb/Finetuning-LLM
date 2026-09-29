WITH base AS (
    SELECT *, INITCAP(TRIM(dept_name)) AS dept_name_title
    FROM departments
    WHERE budget > 2925000
),
BudgetBucketSummary AS (
    SELECT CASE WHEN budget >= 3548000 THEN 'high' WHEN budget >= 1063000 THEN 'mid' ELSE 'low' END AS budget_bucket, SUM(CASE WHEN dept_name = 'Legal' THEN budget ELSE 0 END) AS legal_budget
    FROM base
    GROUP BY CASE WHEN budget >= 3548000 THEN 'high' WHEN budget >= 1063000 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM BudgetBucketSummary
WHERE legal_budget >= 69
ORDER BY legal_budget DESC, budget_bucket
