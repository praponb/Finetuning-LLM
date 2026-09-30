WITH valid_accounts AS (
    SELECT *
    FROM accounts
    WHERE status IN ('OPEN', 'CLOSED') AND account_type IN ('SAVINGS', 'CHECKING')
),
balance_bucket_summary AS (
    SELECT CASE WHEN balance >= 68000 THEN 'high' WHEN balance >= 30000 THEN 'mid' ELSE 'low' END AS balance_bucket, COUNT(*) AS total_count, MAX(balance) AS max_balance
    FROM valid_accounts
    GROUP BY CASE WHEN balance >= 68000 THEN 'high' WHEN balance >= 30000 THEN 'mid' ELSE 'low' END
)
SELECT balance_bucket, total_count, max_balance
FROM balance_bucket_summary
ORDER BY max_balance DESC, balance_bucket
LIMIT 20
