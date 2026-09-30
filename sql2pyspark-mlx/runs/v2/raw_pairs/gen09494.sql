SELECT s.id, s.plan, s.end_date
FROM subscriptions s
WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE credit_limit > 14400);
