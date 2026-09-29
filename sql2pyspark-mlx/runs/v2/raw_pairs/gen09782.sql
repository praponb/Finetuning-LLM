SELECT
    a.account_type,
    a.balance,
    a.status,
    t.account_id,
    t.merchant,
    t.txn_type
FROM accounts a
LEFT JOIN transactions t ON a.id = t.account_id
ORDER BY t.txn_type DESC, a.account_type
