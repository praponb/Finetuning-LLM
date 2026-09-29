SELECT
    t.id,
    t.txn_type,
    a.account_type,
    a.balance,
    a.opened_date
FROM transactions t
LEFT JOIN accounts a ON a.id = t.account_id
ORDER BY t.txn_type DESC
