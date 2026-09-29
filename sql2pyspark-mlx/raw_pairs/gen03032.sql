SELECT
    t.id,
    t.merchant,
    t.txn_type,
    a.status,
    c.age
FROM transactions t
JOIN accounts a ON a.id = t.account_id
FULL OUTER JOIN customers c ON a.customer_id = c.id
WHERE t.account_id IN (3, 39);
