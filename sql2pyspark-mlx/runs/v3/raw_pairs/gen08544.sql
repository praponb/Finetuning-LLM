SELECT
    t.account_id,
    t.txn_time,
    t.txn_type,
    a.balance
FROM transactions t
LEFT JOIN accounts a ON t.account_id = a.id
