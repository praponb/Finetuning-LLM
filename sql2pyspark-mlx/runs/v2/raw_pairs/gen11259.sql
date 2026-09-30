SELECT accounts.id, accounts.opened_date, accounts.customer_id
FROM accounts
WHERE accounts.customer_id IN (SELECT id FROM customers WHERE city = 'New York')
ORDER BY customer_id ASC
