SELECT
    t.is_flagged,
    t.account_id,
    a.customer_id,
    a.status,
    c.segment
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
INNER JOIN customers c ON a.customer_id = c.id
ORDER BY a.status
