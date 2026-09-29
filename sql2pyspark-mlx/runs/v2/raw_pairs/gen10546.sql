WITH account_summary AS (
    SELECT account_id, ROUND((100 * SUM(CASE WHEN txn_type = 'REFUND' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_refund, COUNT(amount) amount_count
    FROM transactions
    WHERE amount BETWEEN 1200 AND 3600
    GROUP BY account_id
)
SELECT
    a.id,
    a.balance,
    as.pct_refund,
    as.amount_count
FROM accounts a
LEFT JOIN account_summary as ON a.id = as.account_id
ORDER BY pct_refund;
