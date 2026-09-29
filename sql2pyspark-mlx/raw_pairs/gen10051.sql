SELECT c.id, c.city FROM customers c WHERE NOT EXISTS (SELECT 1 FROM subscriptions s WHERE s.customer_id = c.id AND YEAR(s.start_date) = 2023);
