SELECT
    budget_bucket,
    location,
    total_budget,
    max_budget
FROM (
    SELECT
        CASE WHEN budget >= 3350000 THEN 'high' WHEN budget >= 1665000 THEN 'mid' ELSE 'low' END budget_bucket,
        location,
        SUM(budget) total_budget,
        MAX(budget) max_budget
    FROM departments
    WHERE location IN ('Berlin', 'Bangkok', 'Tokyo')
    GROUP BY CASE WHEN budget >= 3350000 THEN 'high' WHEN budget >= 1665000 THEN 'mid' ELSE 'low' END, location
) t
WHERE max_budget >= 3237000;
