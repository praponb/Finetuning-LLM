SELECT c.id, c.credit_limit FROM customers c WHERE NOT EXISTS (SELECT 1 FROM subscriptions s WHERE s.customer_id = c.id AND s.start_date <= '2024-03-20')
