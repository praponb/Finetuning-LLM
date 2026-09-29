SELECT c.id, c.age, c.country FROM customers c WHERE c.credit_limit < 14600 AND NOT EXISTS (SELECT 1 FROM subscriptions s WHERE s.customer_id = c.id);
