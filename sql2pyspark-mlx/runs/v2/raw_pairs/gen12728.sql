SELECT c.id, c.city FROM customers c WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.customer_id = c.id) ORDER BY city DESC, id LIMIT 5
