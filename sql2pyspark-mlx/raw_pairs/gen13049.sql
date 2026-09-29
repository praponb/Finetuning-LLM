SELECT
    a.status,
    t.txn_type,
    t.account_id,
    t.merchant
FROM accounts a
LEFT JOIN transactions t ON a.id = t.account_id;
