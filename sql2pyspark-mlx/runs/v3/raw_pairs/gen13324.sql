SELECT
    t.id,
    t.txn_type,
    a.opened_date,
    a.customer_id,
    c.email
FROM transactions t
LEFT JOIN accounts a ON t.account_id = a.id
LEFT JOIN customers c ON a.customer_id = c.id
WHERE c.is_active
ORDER BY t.txn_type DESC, id;
