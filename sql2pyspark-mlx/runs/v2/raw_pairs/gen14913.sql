SELECT
    a.customer_id,
    t.account_id,
    t.txn_time,
    t.is_flagged
FROM accounts a
LEFT JOIN transactions t ON a.id = t.account_id
ORDER BY t.txn_time
