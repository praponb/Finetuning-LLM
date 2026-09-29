SELECT s.id, s.monthly_fee
FROM subscriptions s
WHERE s.customer_id IN (SELECT id FROM customers WHERE credit_limit < 6000)
ORDER BY monthly_fee DESC
