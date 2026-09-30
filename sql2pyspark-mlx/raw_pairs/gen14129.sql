SELECT c.id, c.name, c.city FROM customers c WHERE c.signup_date < '2025-10-12' AND NOT EXISTS (SELECT 1 FROM accounts a WHERE a.customer_id = c.id AND a.customer_id IS NULL);
