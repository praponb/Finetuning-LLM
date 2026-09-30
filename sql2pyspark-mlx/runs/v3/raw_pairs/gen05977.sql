SELECT s.id, s.start_date, s.monthly_fee
FROM subscriptions s
WHERE s.customer_id IN (SELECT id FROM customers WHERE credit_limit > 6200)
