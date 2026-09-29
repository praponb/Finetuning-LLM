WITH valid_departments AS (
    SELECT *
    FROM departments
    WHERE budget BETWEEN 1822000 AND 2229000
),
summary AS (
    SELECT CASE WHEN budget >= 4398000 THEN 'high' WHEN budget >= 1249000 THEN 'mid' ELSE 'low' END AS budget_bucket, ROUND(AVG(budget), 2) AS avg_budget, COUNT(budget) AS budget_count
    FROM valid_departments
    GROUP BY CASE WHEN budget >= 4398000 THEN 'high' WHEN budget >= 1249000 THEN 'mid' ELSE 'low' END
)
SELECT budget_bucket, avg_budget, budget_count
FROM summary
WHERE avg_budget >= 40
ORDER BY budget_count;
