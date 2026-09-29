SELECT
    a.id,
    a.opened_date,
    a.balance,
    c.email,
    c.credit_limit,
    c.name
FROM accounts a
RIGHT JOIN customers c ON a.customer_id = c.id
WHERE c.name LIKE 'A%';
