SELECT
    a.balance,
    t.txn_type,
    t.merchant,
    t.txn_time
FROM accounts a
JOIN transactions t ON t.account_id = a.id;
