WITH account_stats AS (
    SELECT account_id, SUM(CASE WHEN txn_type = 'FEE' THEN amount ELSE 0 END) AS fee_amount
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.id,
    a.balance,
    a.status,
    as.fee_amount
FROM accounts a
INNER JOIN account_stats as ON a.id = as.account_id
WHERE as.fee_amount > 63
ORDER BY fee_amount
