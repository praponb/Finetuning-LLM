SELECT
    t.account_id,
    a.balance,
    a.opened_date,
    c.age,
    c.name
FROM transactions t
LEFT JOIN accounts a ON a.id = t.account_id
LEFT JOIN customers c ON a.customer_id = c.id;
