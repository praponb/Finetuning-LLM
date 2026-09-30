SELECT s.id, s.end_date, s.plan FROM subscriptions s WHERE s.customer_id IN (SELECT id FROM customers WHERE is_active = TRUE);
