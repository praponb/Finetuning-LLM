SELECT
    t.is_flagged,
    t.txn_time,
    a.opened_date,
    a.customer_id,
    c.age
FROM transactions t
RIGHT JOIN accounts a ON a.id = t.account_id
INNER JOIN customers c ON a.customer_id = c.id
WHERE a.status IN ('OPEN', 'FROZEN', 'CLOSED');
