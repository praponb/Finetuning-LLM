SELECT c.id, c.age, c.country, c.is_active FROM customers c WHERE EXISTS (SELECT 1 FROM subscriptions s WHERE s.customer_id = c.id)
