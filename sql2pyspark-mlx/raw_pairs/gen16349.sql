SELECT s.id, s.status, s.start_date
FROM subscriptions s
WHERE s.customer_id IN (SELECT id FROM customers WHERE is_active)
