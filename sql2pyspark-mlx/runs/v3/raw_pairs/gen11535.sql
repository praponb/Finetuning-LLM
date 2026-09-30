SELECT
    a.id,
    a.customer_id,
    a.balance,
    a.status
FROM accounts a
WHERE a.customer_id IN (SELECT id FROM customers WHERE city != 'London')
