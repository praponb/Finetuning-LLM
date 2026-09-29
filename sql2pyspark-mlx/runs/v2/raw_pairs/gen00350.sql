SELECT *
FROM (
    SELECT CASE WHEN budget >= 4264000 THEN 'high' WHEN budget >= 645000 THEN 'mid' ELSE 'low' END budget_bucket, SUM(budget) total_budget
    FROM departments
    GROUP BY CASE WHEN budget >= 4264000 THEN 'high' WHEN budget >= 645000 THEN 'mid' ELSE 'low' END
) x
WHERE total_budget < 15202000
ORDER BY budget_bucket;
