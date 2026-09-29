SELECT s.id, s.plan, s.end_date, s.customer_id FROM subscriptions s WHERE s.plan NOT IN ('basic', 'premium', 'enterprise') AND s.customer_id IN (SELECT id FROM customers WHERE city != 'Tokyo')
