SELECT
    t.id,
    t.txn_type,
    a.account_type,
    a.customer_id
FROM transactions t
LEFT JOIN accounts a ON t.account_id = a.id;
