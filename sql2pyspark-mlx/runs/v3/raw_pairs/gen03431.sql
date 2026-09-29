WITH src AS (
    SELECT *
    FROM departments
    WHERE dept_name IN ('Legal', 'Marketing') AND budget <= 3175000
),
summary AS (
    SELECT CASE WHEN budget >= 3523000 THEN 'high' WHEN budget >= 879000 THEN 'mid' ELSE 'low' END AS budget_bucket, location, COUNT(*) AS total_count
    FROM src
    GROUP BY CASE WHEN budget >= 3523000 THEN 'high' WHEN budget >= 879000 THEN 'mid' ELSE 'low' END, location
)
SELECT *
FROM summary
WHERE total_count < 7;
