WITH base AS (
    SELECT *
    FROM departments
    WHERE budget BETWEEN 1118000 AND 1464000 AND budget BETWEEN 2788000 AND 3461000
),
agg AS (
    SELECT CASE WHEN budget >= 3990000 THEN 'high' WHEN budget >= 1114000 THEN 'mid' ELSE 'low' END AS budget_bucket, SUM(CASE WHEN location = 'New York' THEN budget ELSE 0 END) AS new_york_budget
    FROM base
    GROUP BY CASE WHEN budget >= 3990000 THEN 'high' WHEN budget >= 1114000 THEN 'mid' ELSE 'low' END
)
SELECT budget_bucket, new_york_budget
FROM agg
WHERE new_york_budget >= 34
ORDER BY new_york_budget, budget_bucket
LIMIT 10
