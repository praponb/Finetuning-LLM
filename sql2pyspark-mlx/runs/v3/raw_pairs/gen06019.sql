SELECT c.id, c.signup_date FROM customers c WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.customer_id = c.id AND a.opened_date <= '2024-12-05') ORDER BY c.id DESC
