SELECT c.id, c.signup_date, c.segment, c.is_active FROM customers c WHERE EXISTS (SELECT 1 FROM accounts a WHERE a.customer_id = c.id AND a.customer_id IN (13, 30))
