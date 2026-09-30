SELECT c.id, c.city, c.age, c.credit_limit FROM customers c WHERE EXISTS (SELECT 1 FROM accounts a WHERE a.customer_id = c.id AND a.status NOT IN ('FROZEN', 'CLOSED'))
