SELECT
    t.id,
    t.is_flagged,
    t.amount,
    t.txn_type,
    a.customer_id,
    a.account_type,
    a.status,
    CEIL(a.balance) AS balance_whole
FROM transactions t
JOIN accounts a ON t.account_id = a.id
ORDER BY a.customer_id DESC
