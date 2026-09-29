SELECT a.id, a.opened_date, a.balance
FROM accounts a
WHERE a.customer_id IN (SELECT id FROM customers WHERE name LIKE '%Mia%')
