SELECT *
FROM (
    SELECT dept_name, CASE WHEN budget >= 3797000 THEN 'high' WHEN budget >= 804000 THEN 'mid' ELSE 'low' END AS budget_bucket, MIN(budget) AS min_budget
    FROM departments
    WHERE location = 'Chiang Mai'
    GROUP BY dept_name, CASE WHEN budget >= 3797000 THEN 'high' WHEN budget >= 804000 THEN 'mid' ELSE 'low' END
) AS s
WHERE min_budget >= 2440000
ORDER BY min_budget, dept_name, budget_bucket
LIMIT 10
