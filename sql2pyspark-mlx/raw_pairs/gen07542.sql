SELECT
    t.txn_time,
    t.amount,
    a.account_type,
    a.status,
    a.customer_id
FROM transactions t
RIGHT JOIN accounts a ON t.account_id = a.id;
