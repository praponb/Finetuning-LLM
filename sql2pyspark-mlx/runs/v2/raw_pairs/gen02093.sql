SELECT
    a.id,
    a.account_type,
    a.customer_id,
    t.min_amount,
    t.refund_amount
FROM accounts a
LEFT JOIN (SELECT account_id, MIN(amount) AS min_amount, SUM(CASE WHEN txn_type = 'REFUND' THEN amount ELSE 0 END) AS refund_amount
    FROM transactions
    WHERE txn_type = 'DEBIT'
    GROUP BY account_id) t ON a.id = t.account_id
WHERE t.refund_amount > 53;
