SELECT a.id, a.customer_id
FROM accounts a
WHERE a.balance > 37000 AND a.customer_id IN (SELECT id FROM customers WHERE age != 55)
ORDER BY a.customer_id DESC
