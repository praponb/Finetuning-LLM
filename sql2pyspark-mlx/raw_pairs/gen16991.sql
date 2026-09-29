SELECT s.id, s.start_date, s.customer_id, s.plan FROM subscriptions s WHERE s.customer_id IN (24, 35, 37, 38) AND s.customer_id IN (SELECT id FROM customers WHERE signup_date >= '2025-01-27')
