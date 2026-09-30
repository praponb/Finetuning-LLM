SELECT c.id, c.country, c.name, c.segment FROM customers c WHERE EXISTS (SELECT 1 FROM accounts a WHERE a.customer_id = c.id AND a.customer_id IN (13, 28, 31, 40));
