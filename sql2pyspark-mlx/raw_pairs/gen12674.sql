SELECT budget_bucket, total_count, stddev_budget
FROM (
    SELECT CASE WHEN budget >= 4393000 THEN 'high' WHEN budget >= 2055000 THEN 'mid' ELSE 'low' END budget_bucket, COUNT(*) total_count, ROUND(STDDEV(budget), 2) stddev_budget
    FROM departments
    GROUP BY CASE WHEN budget >= 4393000 THEN 'high' WHEN budget >= 2055000 THEN 'mid' ELSE 'low' END
) agg
WHERE total_count > 6
ORDER BY stddev_budget, budget_bucket
LIMIT 3
