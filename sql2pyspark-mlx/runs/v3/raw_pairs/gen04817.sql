SELECT
    a.balance,
    a.customer_id,
    t.txn_type,
    t.txn_time
FROM accounts a
LEFT JOIN transactions t ON a.id = t.account_id
ORDER BY a.customer_id ASC, txn_type DESC;
