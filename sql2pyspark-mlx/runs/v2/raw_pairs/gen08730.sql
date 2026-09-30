SELECT
    t.id,
    t.txn_type,
    t.amount,
    a.status,
    a.balance,
    c.name,
    c.segment,
    LENGTH(t.txn_type) txn_type_length
FROM transactions t
INNER JOIN accounts a ON a.id = t.account_id
INNER JOIN customers c ON a.customer_id = c.id;
