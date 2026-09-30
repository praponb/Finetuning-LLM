SELECT
    t.id,
    t.txn_type,
    a.opened_date,
    c.credit_limit
FROM transactions t
LEFT JOIN accounts a ON t.account_id = a.id
INNER JOIN customers c ON a.customer_id = c.id
ORDER BY a.opened_date, id
LIMIT 10
