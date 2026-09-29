WITH base AS (
    SELECT *, TRIM(status) AS clean_status
    FROM accounts
    WHERE balance > 43000 AND status IN ('CLOSED', 'FROZEN', 'OPEN') AND opened_date < '2025-02-01'
),
accounts_stats AS (
    SELECT status, CASE WHEN balance >= 59000 THEN 'high' WHEN balance >= 29000 THEN 'mid' ELSE 'low' END AS balance_bucket, AVG(balance) AS avg_balance
    FROM base
    GROUP BY status, CASE WHEN balance >= 59000 THEN 'high' WHEN balance >= 29000 THEN 'mid' ELSE 'low' END
)
SELECT status, balance_bucket, avg_balance
FROM accounts_stats
WHERE avg_balance < 18000
ORDER BY status DESC;
