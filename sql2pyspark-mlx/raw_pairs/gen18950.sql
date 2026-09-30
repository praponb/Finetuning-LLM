WITH src AS (
    SELECT *
    FROM departments
    WHERE location IN ('New York', 'Tokyo', 'London') OR location IN ('Bangkok', 'Singapore', 'New York')
),
summary AS (
    SELECT CASE WHEN budget >= 3617000 THEN 'high' WHEN budget >= 1086000 THEN 'mid' ELSE 'low' END AS budget_bucket, MAX(budget) AS max_budget, MIN(budget) AS min_budget
    FROM src
    GROUP BY CASE WHEN budget >= 3617000 THEN 'high' WHEN budget >= 1086000 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM summary
WHERE max_budget >= 1305000
ORDER BY min_budget, budget_bucket
LIMIT 3
