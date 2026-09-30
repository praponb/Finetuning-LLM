WITH src AS (
    SELECT *
    FROM departments
    WHERE budget <= 3553000 AND (location = 'Bangkok' OR budget BETWEEN 1142000 AND 1724000)
),
BudgetBucketSummary AS (
    SELECT
        CASE WHEN budget >= 3695000 THEN 'high' WHEN budget >= 1034000 THEN 'mid' ELSE 'low' END AS budget_bucket,
        dept_name,
        SUM(CASE WHEN location = 'Berlin' THEN budget ELSE 0 END) AS berlin_budget,
        ROUND(STDDEV(budget), 2) AS stddev_budget
    FROM src
    GROUP BY CASE WHEN budget >= 3695000 THEN 'high' WHEN budget >= 1034000 THEN 'mid' ELSE 'low' END, dept_name
)
SELECT
    budget_bucket,
    dept_name,
    berlin_budget,
    stddev_budget
FROM BudgetBucketSummary
WHERE stddev_budget >= 73
ORDER BY dept_name, berlin_budget, budget_bucket
LIMIT 20
