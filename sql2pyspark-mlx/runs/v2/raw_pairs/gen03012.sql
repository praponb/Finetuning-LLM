SELECT
    t.id,
    t.amount,
    a.status,
    a.customer_id
FROM transactions t
RIGHT JOIN accounts a ON a.id = t.account_id
