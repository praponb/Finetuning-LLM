WITH recent_customers AS (
    SELECT *, CAST(is_active AS INT) AS is_active_flag
    FROM customers
    WHERE credit_limit IS NULL AND NOT is_active
),
CustomersStats AS (
    SELECT CASE WHEN age >= 72 THEN 'high' WHEN age >= 28 THEN 'mid' ELSE 'low' END AS age_bucket, CASE WHEN credit_limit >= 16300 THEN 'high' WHEN credit_limit >= 3000 THEN 'mid' ELSE 'low' END AS credit_limit_bucket, SUM(is_active_flag) AS total_is_active_flag
    FROM recent_customers
    GROUP BY CASE WHEN age >= 72 THEN 'high' WHEN age >= 28 THEN 'mid' ELSE 'low' END, CASE WHEN credit_limit >= 16300 THEN 'high' WHEN credit_limit >= 3000 THEN 'mid' ELSE 'low' END
)
SELECT age_bucket, credit_limit_bucket, total_is_active_flag
FROM CustomersStats
ORDER BY total_is_active_flag DESC
