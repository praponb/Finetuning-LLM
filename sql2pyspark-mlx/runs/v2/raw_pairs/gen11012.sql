WITH base AS (
    SELECT *, COALESCE(budget, 0) AS budget_filled
    FROM departments
    WHERE dept_name IN ('HR', 'Legal')
),
summary AS (
    SELECT CASE WHEN budget >= 4036000 THEN 'high' WHEN budget >= 1573000 THEN 'mid' ELSE 'low' END AS budget_bucket, AVG(budget_filled) AS avg_budget_filled, ROUND(STDDEV(budget), 2) AS stddev_budget
    FROM base
    GROUP BY CASE WHEN budget >= 4036000 THEN 'high' WHEN budget >= 1573000 THEN 'mid' ELSE 'low' END
)
SELECT budget_bucket, avg_budget_filled, stddev_budget
FROM summary
ORDER BY avg_budget_filled DESC
