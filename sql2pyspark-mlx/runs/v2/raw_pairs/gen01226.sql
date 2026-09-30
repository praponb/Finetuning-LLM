SELECT s.id, s.monthly_fee, s.end_date FROM subscriptions s WHERE s.customer_id IN (SELECT id FROM customers WHERE city IS NOT NULL);
