SELECT c.id, c.is_active FROM customers c WHERE EXISTS (SELECT 1 FROM accounts a WHERE a.customer_id = c.id AND a.status = 'OPEN') ORDER BY id DESC LIMIT 1;
