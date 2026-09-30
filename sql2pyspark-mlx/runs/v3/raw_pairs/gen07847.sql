SELECT
    t.id,
    t.merchant,
    t.amount,
    t.txn_time,
    a.customer_id,
    a.status
FROM transactions t
JOIN accounts a ON t.account_id = a.id
WHERE a.status = 'OPEN' OR t.txn_type = 'DEBIT';
