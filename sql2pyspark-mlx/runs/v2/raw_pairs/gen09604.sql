SELECT
    t.id,
    t.account_id,
    a.opened_date,
    a.customer_id,
    c.segment
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
INNER JOIN customers c ON a.customer_id = c.id
ORDER BY a.customer_id DESC
