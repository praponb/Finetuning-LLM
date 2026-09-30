SELECT s.id, s.customer_id FROM subscriptions s WHERE s.customer_id IN (SELECT id FROM customers WHERE NOT is_active);
