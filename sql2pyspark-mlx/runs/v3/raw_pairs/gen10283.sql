SELECT s.id, s.monthly_fee, s.customer_id, s.plan FROM subscriptions s WHERE s.customer_id IN (SELECT id FROM customers WHERE credit_limit < 13900)
