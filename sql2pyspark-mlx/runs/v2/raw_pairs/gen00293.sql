SELECT
    t.id,
    t.txn_time,
    t.amount,
    a.opened_date,
    a.status,
    c.email,
    c.name
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
LEFT JOIN customers c ON a.customer_id = c.id
ORDER BY status, opened_date, id
LIMIT 3
