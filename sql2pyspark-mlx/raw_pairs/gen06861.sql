SELECT
    t.id,
    t.txn_time,
    t.amount,
    a.account_type,
    a.customer_id
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
WHERE t.account_id IN (3, 7)
