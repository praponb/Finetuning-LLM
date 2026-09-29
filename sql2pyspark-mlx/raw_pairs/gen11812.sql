SELECT s.id, s.plan, s.status
FROM subscriptions s
WHERE s.monthly_fee <= 140 AND s.customer_id NOT IN (SELECT id FROM customers WHERE credit_limit > 12500);
