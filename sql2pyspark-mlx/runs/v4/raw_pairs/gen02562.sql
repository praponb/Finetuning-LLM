SELECT a.id, a.opened_date
FROM accounts a
WHERE a.customer_id IN (SELECT id FROM customers WHERE age >= 65)
