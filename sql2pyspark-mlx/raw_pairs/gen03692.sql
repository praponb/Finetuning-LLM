WITH account_summary AS (
    SELECT account_id, SUM(CASE WHEN txn_type = 'CREDIT' THEN 1 ELSE 0 END) credit_count, MAX(amount) - MIN(amount) amount_range
    FROM transactions
    WHERE merchant = 'Grab'
    GROUP BY account_id
)
SELECT
    a.id,
    a.account_type,
    a.balance,
    as.credit_count,
    as.amount_range
FROM accounts a
JOIN account_summary as ON a.id = as.account_id
WHERE as.amount_range > 39
ORDER BY credit_count DESC, account_type, id
LIMIT 3;
