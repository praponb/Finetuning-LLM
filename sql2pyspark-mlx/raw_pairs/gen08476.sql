SELECT c.id, c.is_active, c.country FROM customers c WHERE c.credit_limit > 6000 AND NOT EXISTS (SELECT 1 FROM subscriptions s WHERE s.customer_id = c.id)
