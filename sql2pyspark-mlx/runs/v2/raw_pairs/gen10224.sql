SELECT *
FROM (
    SELECT CASE WHEN budget >= 3162000 THEN 'high' WHEN budget >= 1514000 THEN 'mid' ELSE 'low' END AS budget_bucket, SUM(CASE WHEN dept_name = 'Engineering' THEN 1 ELSE 0 END) AS engineering_count, SUM(CASE WHEN dept_name = 'Marketing' THEN 1 ELSE 0 END) AS marketing_count
    FROM departments
    GROUP BY CASE WHEN budget >= 3162000 THEN 'high' WHEN budget >= 1514000 THEN 'mid' ELSE 'low' END
) AS s
WHERE engineering_count >= 41
