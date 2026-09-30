SELECT s.id, s.start_date, s.end_date
FROM subscriptions s
WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE country IN ('DE', 'JP', 'UK'));
