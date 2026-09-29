SELECT budget_bucket, row_count
FROM (
    SELECT CASE WHEN budget >= 3271000 THEN 'high' WHEN budget >= 1489000 THEN 'mid' ELSE 'low' END AS budget_bucket, COUNT(*) AS row_count
    FROM departments
    WHERE dept_name = 'Marketing'
    GROUP BY CASE WHEN budget >= 3271000 THEN 'high' WHEN budget >= 1489000 THEN 'mid' ELSE 'low' END
) AS t
WHERE row_count > 6
