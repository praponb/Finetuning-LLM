SELECT
    t.id,
    t.txn_type,
    a.opened_date,
    a.balance,
    c.email
FROM transactions t
RIGHT JOIN accounts a ON t.account_id = a.id
INNER JOIN customers c ON a.customer_id = c.id
WHERE t.amount BETWEEN 2500 AND 2700;
