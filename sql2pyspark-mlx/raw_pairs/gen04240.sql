SELECT a.id, a.customer_id, a.status FROM accounts a WHERE a.customer_id NOT IN (SELECT id FROM customers WHERE name LIKE '%Tay%') ORDER BY id DESC
