SELECT
    t.id,
    t.txn_type,
    t.merchant,
    a.status,
    a.customer_id,
    c.age
FROM transactions t
LEFT JOIN accounts a ON a.id = t.account_id
LEFT JOIN customers c ON a.customer_id = c.id
WHERE HOUR(t.txn_time) BETWEEN 12 AND 15 AND c.segment != 'Corporate'
