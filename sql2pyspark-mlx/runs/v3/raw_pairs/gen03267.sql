SELECT c.id, c.age, c.country, c.city FROM customers c WHERE EXISTS (SELECT 1 FROM accounts a WHERE a.customer_id = c.id)
