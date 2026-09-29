SELECT s.id, s.plan FROM subscriptions s WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE is_active);
