SELECT c.id, c.segment, c.name FROM customers c WHERE EXISTS (SELECT 1 FROM accounts a WHERE a.customer_id = c.id)
