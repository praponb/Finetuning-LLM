WITH account_stats AS (
    SELECT account_id, MAX(amount) - MIN(amount) amount_range, SUM(CASE WHEN merchant = 'Starbucks' THEN amount ELSE 0 END) starbucks_amount
    FROM transactions
    WHERE txn_type IN ('REFUND', 'FEE', 'DEBIT')
    GROUP BY account_id
)
SELECT
    a.id,
    a.status,
    as.amount_range,
    as.starbucks_amount
FROM accounts a
LEFT JOIN account_stats as ON a.id = as.account_id
WHERE as.starbucks_amount > 41
ORDER BY as.amount_range DESC, id DESC;
