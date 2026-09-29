WITH filtered_transactions AS (
    SELECT *
    FROM transactions
    WHERE txn_type = 'CREDIT'
),
merchant_summary AS (
    SELECT merchant, COUNT(CASE WHEN merchant = 'Shell' THEN 1 END) AS shell_count
    FROM filtered_transactions
    GROUP BY merchant
)
SELECT *
FROM merchant_summary
WHERE shell_count >= 9
ORDER BY shell_count DESC;
