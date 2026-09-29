WITH base AS (
    SELECT *, DATE_ADD(opened_date, 79) AS opened_date_plus_79d
    FROM accounts
    WHERE status IN ('CLOSED', 'FROZEN') AND (customer_id = 35 OR customer_id IN (20, 29, 39))
),
balance_bucket_summary AS (
    SELECT CASE WHEN balance >= 84000 THEN 'high' WHEN balance >= 18000 THEN 'mid' ELSE 'low' END AS balance_bucket, COUNT(balance) AS balance_count
    FROM base
    GROUP BY CASE WHEN balance >= 84000 THEN 'high' WHEN balance >= 18000 THEN 'mid' ELSE 'low' END
)
SELECT balance_bucket, balance_count
FROM balance_bucket_summary
