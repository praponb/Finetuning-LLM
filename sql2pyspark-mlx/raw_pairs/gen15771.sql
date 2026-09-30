SELECT
    t.merchant,
    t.amount,
    t.txn_type,
    a.customer_id
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id;
