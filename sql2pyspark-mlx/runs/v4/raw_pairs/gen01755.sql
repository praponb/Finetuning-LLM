SELECT id, customer_id, plan FROM subscriptions WHERE customer_id IS NOT NULL UNION SELECT id, customer_id, plan FROM subscriptions WHERE status = 'EXPIRED' ORDER BY plan DESC;
