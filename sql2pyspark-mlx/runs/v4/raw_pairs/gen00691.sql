SELECT a.id, a.status, a.customer_id
FROM accounts a
WHERE a.customer_id IN (SELECT id FROM customers WHERE name LIKE 'Ali%')
ORDER BY id ASC
