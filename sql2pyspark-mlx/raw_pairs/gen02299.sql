SELECT a.id, a.account_type
FROM accounts a
WHERE a.customer_id NOT IN (SELECT id FROM customers WHERE age = 56)
