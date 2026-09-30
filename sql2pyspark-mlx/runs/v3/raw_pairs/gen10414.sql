SELECT
    a.balance,
    a.customer_id,
    t.txn_time,
    t.account_id,
    t.is_flagged
FROM accounts a
LEFT JOIN transactions t ON a.id = t.account_id
WHERE a.balance <= 78000;
