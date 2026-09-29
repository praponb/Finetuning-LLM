SELECT c.id, c.age FROM customers c WHERE EXISTS (SELECT 1 FROM accounts a WHERE a.customer_id = c.id AND a.status = 'OPEN');
