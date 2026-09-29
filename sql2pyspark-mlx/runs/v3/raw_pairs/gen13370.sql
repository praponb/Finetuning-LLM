WITH Base AS (
    SELECT *
    FROM accounts
    WHERE customer_id IS NULL AND status NOT IN ('OPEN', 'FROZEN', 'CLOSED')
),
agg AS (
    SELECT CASE WHEN balance >= 79000 THEN 'high' WHEN balance >= 17000 THEN 'mid' ELSE 'low' END AS balance_bucket, ROUND(STDDEV(balance), 2) AS stddev_balance
    FROM Base
    GROUP BY CASE WHEN balance >= 79000 THEN 'high' WHEN balance >= 17000 THEN 'mid' ELSE 'low' END
)
SELECT balance_bucket, stddev_balance
FROM agg
ORDER BY stddev_balance
