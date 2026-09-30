SELECT
    t.txn_time,
    t.is_flagged,
    a.account_type,
    a.balance,
    c.is_active,
    c.country
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
LEFT JOIN customers c ON a.customer_id = c.id
ORDER BY a.balance DESC;
